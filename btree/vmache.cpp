#include "vmache.hpp"
#include "common.hpp"

static std::mutex AIO_ERROR_LOCK;
__thread uint16_t workerThreadId = ~0;
__thread int32_t tpcchistorycounter = 0;

void *allocHuge(size_t size) {
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    madvise(p, size, MADV_HUGEPAGE);
    return p;
}

static u64 envOr(const char *env, u64 value) {
    if (getenv(env))
        return atof(getenv(env));
    return value;
}

BufferManager bm;

BufferManager::BufferManager() : virtSize(envOr("VIRTGB", 16) * gb), physSize(envOr("PHYSGB", 4) * gb),
                                 virtCount(virtSize / pageSize), physCount(physSize / pageSize),
                                 residentSet(physCount) {
    assert(virtSize >= physSize);
    const char *path = getenv("BLOCK") ? getenv("BLOCK") : "/tmp/bm";
    blockfd = open(path, O_RDWR | O_DIRECT, S_IRWXU);
    if (blockfd == -1) {
        std::cerr << "cannot open BLOCK device '" << path << "'" << std::endl;
        exit(EXIT_FAILURE);
    }
    u64 virtAllocSize = virtSize + (1
            << 17); // we allocate 128KB (= max len + max offset) extra to prevent segfaults during optimistic reads

    useExmap = envOr("EXMAP", 0);
    if (useExmap) {
        abort();
    } else {
        virtMem = (Page *) mmap(NULL, virtAllocSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);
    }

    pageState = (PageState *) allocHuge(virtCount * sizeof(PageState));
    for (u64 i = 0; i < virtCount; i++)
        pageState[i].init();
    if (virtMem == MAP_FAILED)
        die("mmap failed");

    libaioInterface.reserve(maxWorkerThreads);
    for (unsigned i = 0; i < maxWorkerThreads; i++)
        libaioInterface.emplace_back(LibaioInterface(blockfd, virtMem));

    physUsedCount = 0;
    allocCount = 1; // pid 0 reserved for meta data
    readCount = 0;
    writeCount = 0;
    batch = 32;

    std::cerr << "vmcache " << "blk:" << path << " virtgb:" << virtSize / gb << " physgb:" << physSize / gb << " exmap:"
              << useExmap << std::endl;
}

void BufferManager::ensureFreePages() {
    if (physUsedCount >= physCount * 0.95)
        evict();
}

// allocated new page and fix it
Page *BufferManager::allocPage() {
    physUsedCount++;
    ensureFreePages();
    u64 pid = allocCount++;
    if (pid >= virtCount) {
        std::cerr << "VIRTGB is too low" << std::endl;
        exit(EXIT_FAILURE);
    }
    u64 stateAndVersion = getPageState(pid).stateAndVersion;
    bool succ = getPageState(pid).tryLockX(stateAndVersion);
    assert(succ);
    residentSet.insert(pid);

    if (useExmap) {
        abort();
    }
    virtMem[pid].tagAndDirty.set_dirty(true);

    return virtMem + pid;
}


void BufferManager::handleFault(PID pid) {
    NOSYNC_ABORT;
    physUsedCount++;
    ensureFreePages();
    readPage(pid);
    residentSet.insert(pid);
}

Page *BufferManager::fixX(PID pid) {
    NOSYNC_RET(toPtr(pid));
    PageState &ps = getPageState(pid);
    for (u64 repeatCounter = 0;; repeatCounter++) {
        u64 stateAndVersion = ps.stateAndVersion.load();
        switch (PageState::getState(stateAndVersion)) {
            case PageState::Evicted: {
                if (ps.tryLockX(stateAndVersion)) {
                    handleFault(pid);
                    return virtMem + pid;
                }
                break;
            }
            case PageState::Marked:
            case PageState::Unlocked: {
                if (ps.tryLockX(stateAndVersion)) {
                    return virtMem + pid;
                }
                break;
            }
        }
        vmcache_yield(repeatCounter);
    }
}

Page *BufferManager::fixS(PID pid) {
    NOSYNC_RET(toPtr(pid));
    PageState &ps = getPageState(pid);
    for (u64 repeatCounter = 0;; repeatCounter++) {
        u64 stateAndVersion = ps.stateAndVersion;
        switch (PageState::getState(stateAndVersion)) {
            case PageState::Locked: {
                break;
            }
            case PageState::Evicted: {
                if (ps.tryLockX(stateAndVersion)) {
                    handleFault(pid);
                    ps.unlockX();
                }
                break;
            }
            default: {
                if (ps.tryLockS(stateAndVersion))
                    return virtMem + pid;
            }
        }
        vmcache_yield(repeatCounter);
    }
}

void BufferManager::unfixS(PID pid) {
    NOSYNC_RET();
    getPageState(pid).unlockS();
}

void BufferManager::unfixX(PID pid) {
    NOSYNC_RET();
    getPageState(pid).unlockX();
}

__thread std::atomic<uint64_t> guard_x_count = 0;

void setVmcacheWorkerThreadId(uint16_t x) {
    if (x >= maxWorkerThreads) {
        std::cerr << "too many threads" << std::endl;
        abort();
    }
    workerThreadId = x;
}

void BufferManager::readPage(PID pid) {
    NOSYNC_ABORT;
    if (useExmap) {
        abort();
    } else {
        int ret = pread(blockfd, virtMem + pid, pageSize, pid * pageSize);
        if (ret != pageSize) {
            AIO_ERROR_LOCK.lock();
            std::cout << "error reading page " << pid << ":" << ret;
            if (ret < 0)
                std::cout << ". " << strerror(-ret);
            std::cout << std::endl;
            abort();
        }
        readCount++;
    }
}

void BufferManager::evict() {
    NOSYNC_ABORT;
    std::vector<PID> toEvict;
    toEvict.reserve(batch);
    std::vector<PID> toWrite;
    toWrite.reserve(batch);

    // 0. find candidates, lock dirty ones in shared mode
    while (toEvict.size() + toWrite.size() < batch) {
        residentSet.iterateClockBatch(batch, [&](PID pid) {
            PageState &ps = getPageState(pid);
            u64 v = ps.stateAndVersion;
            switch (PageState::getState(v)) {
                case PageState::Marked:
                    if (virtMem[pid].tagAndDirty.dirty()) {
                        if (ps.tryLockS(v))
                            toWrite.push_back(pid);
                    } else {
                        toEvict.push_back(pid);
                    }
                    break;
                case PageState::Unlocked:
                    ps.tryMark(v);
                    break;
                default:
                    break; // skip
            };
        });
    }

    assert(workerThreadId < maxWorkerThreads);
    // 1. write dirty pages
    libaioInterface[workerThreadId].writePages(toWrite);
    writeCount += toWrite.size();

    // 2. try to lock clean page candidates
    toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](PID pid) {
        PageState &ps = getPageState(pid);
        u64 v = ps.stateAndVersion;
        return (PageState::getState(v) != PageState::Marked) || !ps.tryLockX(v);
    }), toEvict.end());

    // 3. try to upgrade lock for dirty page candidates
    for (auto &pid: toWrite) {
        PageState &ps = getPageState(pid);
        u64 v = ps.stateAndVersion;
        if ((PageState::getState(v) == 1) &&
            ps.stateAndVersion.compare_exchange_weak(v, PageState::sameVersion(v, PageState::Locked)))
            toEvict.push_back(pid);
        else
            ps.unlockS();
    }

    // 4. remove from page table
    if (useExmap) {
        abort();
    } else {
        for (u64 &pid: toEvict)
            madvise(virtMem + pid, pageSize, MADV_DONTNEED);
    }

    // 5. remove from hash table and unlock
    for (u64 &pid: toEvict) {
        bool succ = residentSet.remove(pid);
        assert(succ);
        getPageState(pid).unlockXEvicted();
    }

    physUsedCount -= toEvict.size();
}

void PageState::init() { stateAndVersion.store(sameVersion(0, Unlocked), std::memory_order_release); }

ResidentPageSet::ResidentPageSet(u64 maxCount) : count(next_pow2(maxCount * 1.5)), mask(count - 1), clockPos(0) {
    ht = (Entry *) allocHuge(count * sizeof(Entry));
    memset((void *) ht, 0xFF, count * sizeof(Entry));
}

void LibaioInterface::writePages(const std::vector<PID> &pages) {
    if (pages.size() >= maxIOs) {
        std::cerr << "too many pages to write" << std::endl;
        abort();
    }
    for (u64 i = 0; i < pages.size(); i++) {
        PID pid = pages[i];
        virtMem[pid].tagAndDirty.set_dirty(false);
        cbPtr[i] = &cb[i];
        io_prep_pwrite(cb + i, blockfd, &virtMem[pid], pageSize, pageSize * pid);
    }
    int cnt = io_submit(ctx, pages.size(), cbPtr);
    assert(cnt == pages.size());
    cnt = io_getevents(ctx, pages.size(), pages.size(), events, nullptr);
    assert(cnt == pages.size());
    for (int i = 0; i < cnt; ++i) {
        signed long ret = events[i].res;
        if (ret != pageSize) {
            AIO_ERROR_LOCK.lock();
            std::cout << "error writing page :" << ret;
            if (ret < 0)
                std::cout << ". " << strerror(-ret);
            std::cout << std::endl;
            abort();
        }
    }
}
