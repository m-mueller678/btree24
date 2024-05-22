#pragma once

#include <atomic>
#include <algorithm>
#include <cassert>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>
#include <vector>
#include <span>

#include <errno.h>
#include <libaio.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>
#include "config.hpp"
#include "Tag.hpp"

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef u64 PID; // page id type

#ifdef NOSYNC
#define NOSYNC_RET(x) return x;
#define NOSYNC_ABORT abort();
#else
#define NOSYNC_RET(x) ;
#define NOSYNC_ABORT ;
#endif

struct alignas(pageSize) Page {
    TagAndDirty tagAndDirty;
};

static const int16_t maxWorkerThreads = 256;

#define die(msg) do { perror(msg); exit(EXIT_FAILURE); } while(0)

// allocate memory using huge pages
void *allocHuge(size_t size);

void setVmcacheWorkerThreadId(uint16_t);

// use when lock is not free
inline void vmcache_yield(u64 counter = 0) {
    _mm_pause();
}

struct PageState {
    std::atomic<u64> stateAndVersion;

    static const u64 Unlocked = 0;
    static const u64 MaxShared = 252;
    static const u64 Locked = 253;
    static const u64 Marked = 254;
    static const u64 Evicted = 255;

    PageState() {}

    void init();

    static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) {
        return ((oldStateAndVersion << 8) >> 8) | newState << 56;
    }

    static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) {
        return (((oldStateAndVersion << 8) >> 8) + 1) | newState << 56;
    }

    bool tryLockX(u64 oldStateAndVersion) {
        NOSYNC_RET(true);
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
    }

    void unlockX() {
        NOSYNC_RET();
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
    }

    void unlockXEvicted() {
        NOSYNC_ABORT;
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(), Evicted), std::memory_order_release);
    }

    u64 downgradeXtoO() {
        NOSYNC_RET(stateAndVersion.load());
        auto next = nextVersion(stateAndVersion.load(), Unlocked);
        assert(getState() == Locked);
        stateAndVersion.store(next, std::memory_order_release);
        return next;
    }

    bool tryLockS(u64 oldStateAndVersion) {
        NOSYNC_RET(true);
        u64 s = getState(oldStateAndVersion);
        if (s < MaxShared)
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, s + 1));
        if (s == Marked)
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, 1));
        return false;
    }

    void unlockS() {
        NOSYNC_RET();
        while (true) {
            u64 oldStateAndVersion = stateAndVersion.load();
            u64 state = getState(oldStateAndVersion);
            assert(state > 0 && state <= MaxShared);
            if (stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, state - 1)))
                return;
        }
    }

    bool tryMark(u64 oldStateAndVersion) {
        NOSYNC_RET(true);
        assert(getState(oldStateAndVersion) == Unlocked);
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
    }

    // check if is none of Marked, Locked, or Evicted
    static bool isNotMLE(u64 state) {
        assert(Marked >= 0xfd);
        assert(Evicted >= 0xfd);
        assert(Locked >= 0xfd);
        return state < (((u64) (0xfd)) << 56);
    }

    static u64 getState(u64 v) { return v >> 56; };

    u64 getState() { return getState(stateAndVersion.load()); }

    void operator=(PageState &) = delete;
};

// open addressing hash table used for second chance replacement to keep track of currently-cached pages
struct ResidentPageSet {
    static const u64 empty = ~0ull;
    static const u64 tombstone = (~0ull) - 1;

    struct Entry {
        std::atomic<u64> pid;
    };

    Entry *ht;
    u64 count;
    u64 mask;
    std::atomic<u64> clockPos;

    ResidentPageSet(u64 maxCount);

    ~ResidentPageSet() {
        munmap(ht, count * sizeof(u64));
    }

    u64 next_pow2(u64 x) {
        return 1 << (64 - __builtin_clzl(x - 1));
    }

    u64 hash(u64 k) {
        const u64 m = 0xc6a4a7935bd1e995;
        const int r = 47;
        u64 h = 0x8445d61a4e774912 ^ (8 * m);
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

    void insert(u64 pid) {
        u64 pos = hash(pid) & mask;
        while (true) {
            u64 curr = ht[pos].pid.load();
            assert(curr != pid);
            if ((curr == empty) || (curr == tombstone))
                if (ht[pos].pid.compare_exchange_strong(curr, pid))
                    return;

            pos = (pos + 1) & mask;
        }
    }

    bool remove(u64 pid) {
        u64 pos = hash(pid) & mask;
        while (true) {
            u64 curr = ht[pos].pid.load();
            if (curr == empty)
                return false;

            if (curr == pid)
                if (ht[pos].pid.compare_exchange_strong(curr, tombstone))
                    return true;

            pos = (pos + 1) & mask;
        }
    }

    template<class Fn>
    void iterateClockBatch(u64 batch, Fn fn) {
        u64 pos, newPos;
        do {
            pos = clockPos.load();
            newPos = (pos + batch) % count;
        } while (!clockPos.compare_exchange_strong(pos, newPos));

        for (u64 i = 0; i < batch; i++) {
            u64 curr = ht[pos].pid.load();
            if ((curr != tombstone) && (curr != empty))
                fn(curr);
            pos = (pos + 1) & mask;
        }
    }
};

// libaio interface used to write batches of pages
struct LibaioInterface {
    static const u64 maxIOs = 256;

    int blockfd;
    Page *virtMem;
    io_context_t ctx;
    iocb cb[maxIOs];
    iocb *cbPtr[maxIOs];
    io_event events[maxIOs];

    LibaioInterface(int blockfd, Page *virtMem) : blockfd(blockfd), virtMem(virtMem) {
        memset(&ctx, 0, sizeof(io_context_t));
        int ret = io_setup(maxIOs, &ctx);
        if (ret != 0) {
            std::cerr << "libaio io_setup error: " << ret << " ";
            switch (-ret) {
                case EAGAIN:
                    std::cerr << "EAGAIN";
                    break;
                case EFAULT:
                    std::cerr << "EFAULT";
                    break;
                case EINVAL:
                    std::cerr << "EINVAL";
                    break;
                case ENOMEM:
                    std::cerr << "ENOMEM";
                    break;
                case ENOSYS:
                    std::cerr << "ENOSYS";
                    break;
            };
            exit(EXIT_FAILURE);
        }
    }

    void writePages(const std::vector<PID> &pages) {
        assert(pages.size() < maxIOs);
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
    }
};

struct BufferManager {
    static const u64 mb = 1024ull * 1024;
    static const u64 gb = 1024ull * 1024 * 1024;
    u64 virtSize;
    u64 physSize;
    u64 virtCount;
    u64 physCount;
    struct exmap_user_interface *exmapInterface[maxWorkerThreads];
    std::vector<LibaioInterface> libaioInterface;

    bool useExmap;
    int blockfd;
    int exmapfd;

    std::atomic<u64> physUsedCount;
    ResidentPageSet residentSet;
    std::atomic<u64> allocCount;

    std::atomic<u64> readCount;
    std::atomic<u64> writeCount;

    Page *virtMem;
    PageState *pageState;
    u64 batch;

    PageState &getPageState(PID pid) {
        return pageState[pid];
    }

    BufferManager();

    uint64_t hashPage(PID pid) {
        uint64_t hash;
        std::hash<std::string_view> hasher;
        hash = hasher(std::string_view{reinterpret_cast<const char *>(virtMem + pid), pageSize});
        return hash;
    }

    ~BufferManager() {}

    Page *fixX(PID pid);

    void unfixX(PID pid);

    Page *fixS(PID pid);

    void unfixS(PID pid);

    bool isValidPtr(void *page) { return (page >= virtMem) && (page < (virtMem + virtSize + 16)); }

    PID toPID(void *page) { return reinterpret_cast<Page *>(page) - virtMem; }

    Page *toPtr(PID pid) { return virtMem + pid; }

    void ensureFreePages();

    Page *allocPage();

    void handleFault(PID pid);

    void readPage(PID pid);

    void evict();
};


extern BufferManager bm;

struct OLCRestartException {
};

template<class T>
struct GuardO {
    T *ptr;
    u64 version;
    static const u64 moved = ~0ull;

    static GuardO released() {
        return {};
    }

    PID pid() {
        return bm.toPID(ptr);
    };

    // constructor
    explicit GuardO(u64 pid) : ptr(reinterpret_cast<T *>(bm.toPtr(pid))) {
        init();
    }

    explicit GuardO(T *ptr, u64 version) : ptr(ptr), version(version) {}

    template<class T2>
    GuardO(u64 pid, GuardO<T2> &parent) {
        parent.checkVersionAndRestart();
        ptr = reinterpret_cast<T *>(bm.toPtr(pid));
        init();
    }

    GuardO(GuardO &&other) {
        ptr = other.ptr;
        version = other.version;
    }

    void init() {
        assert(ptr);
        PageState &ps = bm.getPageState(pid());
        for (u64 repeatCounter = 0;; repeatCounter++) {
            u64 v = ps.stateAndVersion.load();
            if (PageState::isNotMLE(v)) [[likely]] {
                version = v;
                return;
            }
            switch (PageState::getState(v)) {
                case PageState::Marked: {
                    u64 newV = PageState::sameVersion(v, PageState::Unlocked);
                    if (ps.stateAndVersion.compare_exchange_weak(v, newV)) {
                        version = newV;
                        return;
                    }
                    break;
                }
                case PageState::Locked:
                    break;
                case PageState::Evicted:
                    if (ps.tryLockX(v)) {
                        bm.handleFault(pid());
                        bm.unfixX(pid());
                    }
                    break;
                default:
                    abort();
            }
            vmcache_yield(repeatCounter);
        }
    }

    // move assignment operator
    GuardO &operator=(GuardO &&other) {
        checkVersionAndRestart();
        ptr = other.ptr;
        version = other.version;
        other.ptr = nullptr;
        return *this;
    }

    // assignment operator
    GuardO &operator=(const GuardO &) = delete;

    // copy constructor
    GuardO(const GuardO &) = delete;

    void checkVersionAndRestart() {
        if (ptr) {
            PageState &ps = bm.getPageState(pid());
            u64 stateAndVersion = ps.stateAndVersion.load();
            if (version == stateAndVersion) [[likely]]// fast path, nothing changed
                return;
            if ((stateAndVersion << 8) == (version << 8)) { // same version
                u64 state = PageState::getState(stateAndVersion);
                if (state <= PageState::MaxShared)
                    return; // ignore shared locks
                if (state == PageState::Marked)
                    if (ps.stateAndVersion.compare_exchange_weak(stateAndVersion,
                                                                 PageState::sameVersion(stateAndVersion,
                                                                                        PageState::Unlocked)))
                        return; // mark cleared
            }
            if (std::uncaught_exceptions() == 0)
                throw OLCRestartException();
        }
    }

    // destructor
    ~GuardO() noexcept(false) {
        checkVersionAndRestart();
    }

    T *operator->() {
        assert(ptr);
        return ptr;
    }

    void release_ignore() {
        ptr = nullptr;
    }

    void release() {
        checkVersionAndRestart();
        ptr = nullptr;
    }

private:
    GuardO() : ptr(nullptr) {}
};

extern __thread std::atomic<uint64_t> guard_x_count;

template<class T>
struct GuardX {
    T *ptr;
    static const u64 moved = ~0ull;

    // constructor
    GuardX() : ptr(nullptr) {}

    // move constructor
    GuardX(GuardX &&other) : ptr(other.ptr) {
        other.ptr = nullptr;
    }

    PID pid() {
        return bm.toPID(ptr);
    }

    // constructor
    explicit GuardX(u64 pid) {
        ptr = reinterpret_cast<T *>(bm.fixX(pid));
        reinterpret_cast<Page *>(ptr)->tagAndDirty.set_dirty(true);
        guard_x_count.fetch_add(1, std::memory_order::relaxed);
    }

    explicit GuardX(GuardO<T> &&other) {
        assert(other.ptr);
        for (u64 repeatCounter = 0;; repeatCounter++) {
            PageState &ps = bm.getPageState(other.pid());
            u64 stateAndVersion = ps.stateAndVersion;
            if ((stateAndVersion << 8) != (other.version << 8))
                throw OLCRestartException();
            u64 state = PageState::getState(stateAndVersion);
            if ((state == PageState::Unlocked) || (state == PageState::Marked)) {
                if (ps.tryLockX(stateAndVersion)) {
                    ptr = other.ptr;
                    reinterpret_cast<Page *>(ptr)->tagAndDirty.set_dirty(true);
                    other.ptr = nullptr;
                    guard_x_count.fetch_add(1, std::memory_order::relaxed);
                    return;
                }
            }
            vmcache_yield(repeatCounter);
        }
    }

    GuardO<T> downgrade() &&{
        GuardO<T> other{ptr, bm.getPageState(pid()).downgradeXtoO()};
        ptr = nullptr;
        guard_x_count.fetch_sub(1, std::memory_order::relaxed);
        return other;
    }

    static GuardX alloc() {
        GuardX r;
        r.ptr = reinterpret_cast<T *>(bm.allocPage());
        guard_x_count.fetch_add(1, std::memory_order::relaxed);
        return r;
    }

    // assignment operator
    GuardX &operator=(const GuardX &) = delete;

    // move assignment operator
    GuardX &operator=(GuardX &&other) {
        if (ptr) {
            bm.unfixX(pid());
            guard_x_count.fetch_sub(1, std::memory_order::relaxed);
        }
        ptr = other.ptr;
        other.ptr = nullptr;
        return *this;
    }

    // copy constructor
    GuardX(const GuardX &) = delete;

    // destructor
    ~GuardX() {
        if (ptr) {
            guard_x_count.fetch_sub(1, std::memory_order::relaxed);
            bm.unfixX(pid());
        }
    }

    T *operator->() {
        assert(ptr);
        return ptr;
    }

    void release() {
        if (ptr) {
            bm.unfixX(pid());
            guard_x_count.fetch_sub(1, std::memory_order::relaxed);
            ptr = nullptr;
        }
    }
};

template<class T>
struct AllocGuard : public GuardX<T> {
    template<typename ...Params>
    AllocGuard(Params &&... params) {
        GuardX<T>::ptr = reinterpret_cast<T *>(bm.allocPage());
        new(GuardX<T>::ptr) T(std::forward<Params>(params)...);
    }
};


template<class T>
struct GuardS {
    T *ptr;
    static const u64 moved = ~0ull;

    // constructor
    explicit GuardS(u64 pid) {
        ptr = reinterpret_cast<T *>(bm.fixS(pid));
    }

    GuardS(GuardO<T> &&other) {
        assert(other.ptr);
        if (bm.getPageState(other.pid()).tryLockS(other.version)) { // XXX: optimize?
            ptr = other.ptr;
            other.ptr = nullptr;
        } else {
            throw OLCRestartException();
        }
    }

    GuardS(GuardS &&other) {
        if (ptr)
            bm.unfixS(pid());
        ptr = other.ptr;
        other.pid = moved;
        other.ptr = nullptr;
    }

    PID pid() {
        return bm.toPID(ptr);
    }

    // assignment operator
    GuardS &operator=(const GuardS &) = delete;

    // move assignment operator
    GuardS &operator=(GuardS &&other) {
        if (ptr)
            bm.unfixS(pid());
        ptr = other.ptr;
        other.ptr = nullptr;
        return *this;
    }

    // copy constructor
    GuardS(const GuardS &) = delete;

    // destructor
    ~GuardS() {
        if (ptr)
            bm.unfixS(pid());
    }

    T *operator->() {
        assert(ptr);
        return ptr;
    }

    void release() {
        if (ptr) {
            bm.unfixS(pid());
            ptr = nullptr;
        }
    }
};