#include <algorithm>
#include <csignal>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <functional>
#include "PerfEvent.hpp"
#include "common.hpp"
#include <iostream>

#include <zipfc.h>
#include <barrier>

using namespace std;

static ZipfcRng *ZIPFC_RNG = nullptr;

void runLargeInsert(unsigned int threadCount, unsigned int duration);

static uint64_t envu64(const char *env) {
    if (getenv(env))
        return strtod(getenv(env), nullptr);
    std::cerr << "missing env " << env << std::endl;
    abort();
}

static double envf64(const char *env) {
    if (getenv(env))
        return strtod(getenv(env), nullptr);
    std::cerr << "missing env " << env << std::endl;
    abort();
}

static uint8_t *makePayload(unsigned len) {
    // add one to support zero length payload
    uint8_t *payload = new uint8_t[len + 1];
    memset(payload, 42, len);
    return payload;
}

static bool isDataInt(BTreeCppPerfEvent &e) {
    auto name = e.params["data_name"];
    return name == "int" || name == "rng4";
}

static bool keySizeAcceptable(unsigned maxPayload, vector<string> &data) {
    for (auto &k: data) {
        if (k.size() + maxPayload > maxKvSize)
            return false;
    }
    return true;
}

static unsigned rangeStart(uint64_t start, uint64_t end, uint64_t nthread, uint64_t tid) {
    if (tid == nthread)
        return end;
    else
        return start + (end - start) * tid / nthread;
}

static void runMulti(BTreeCppPerfEvent e,
                     Key *data,
                     unsigned keyCount,
                     unsigned payloadSize,
                     unsigned workDuration,
                     double zipfParameter,
                     unsigned maxScanLength,
                     unsigned threadCount,
                     bool preInsert
) {
    constexpr unsigned index_samples = 1 << 25;
    uint32_t *zipfIndices = generate_zipf_indices(ZIPFC_RNG, keyCount, zipfParameter, index_samples);

    uint8_t *payloadPtr = makePayload(payloadSize);
    std::span payload{payloadPtr, payloadSize};
    unsigned preInsertCount = keyCount - keyCount / 10;
    std::atomic_bool keepWorking = true;
    std::atomic<uint64_t> ops_performed = 0;

    DataStructureWrapper t(isDataInt(e));

    setVmcacheWorkerThreadId(threadCount);
    std::vector<std::thread> threads;
    std::barrier barrier{threadCount + 1};
    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            setVmcacheWorkerThreadId(tid);
            uint8_t outBuffer[maxKvSize];
            unsigned threadIndexOffset = index_samples / threadCount * tid;
            unsigned local_ops_performed = 0;
            if (preInsert) {
                for (uint64_t i = rangeStart(0, preInsertCount, threadCount, tid);
                     i < rangeStart(0, preInsertCount, threadCount, tid + 1); i++) {
                    t.insert(data[i].span(), payload);
                }
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            //insert
            if (preInsert) {
                for (uint64_t i = rangeStart(preInsertCount, keyCount, threadCount, tid);
                     i < rangeStart(preInsertCount, keyCount, threadCount, tid + 1); i++) {
                    t.insert(data[i].span(), payload);
                }
            } else {
                for (uint64_t i = rangeStart(0, keyCount, threadCount, tid);
                     i < rangeStart(0, keyCount, threadCount, tid + 1); i++) {
                    t.insert(data[i].span(), payload);
                }
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            // ycsb-c
            while (keepWorking.load(std::memory_order::relaxed)) {
                unsigned keyIndex = zipfIndices[(threadIndexOffset + local_ops_performed) % index_samples];
                assert(keyIndex < keyCount);
                if (!t.lookup(data[keyIndex].span())) {
                    std::cout << "missing key " << keyIndex << std::endl;
                    abort();
                }
                local_ops_performed += 1;
            }
            ops_performed += local_ops_performed;
            barrier.arrive_and_wait();
            local_ops_performed = 0;
            std::minstd_rand local_rng(tid);
            std::uniform_int_distribution range_len_distribution(unsigned(1), maxScanLength);
            barrier.arrive_and_wait();
            // ycsb-e
            while (keepWorking.load(std::memory_order::relaxed)) {
                unsigned index = zipfIndices[(threadIndexOffset + local_ops_performed) % index_samples];
                assert(index < keyCount);
                unsigned scanLength = range_len_distribution(local_rng);
                while (keepWorking.load(std::memory_order::relaxed)) {
                    unsigned scanCount = 0;
                    try {
                        t.range_lookup(data[index].span(), outBuffer, [&](unsigned keyLen, std::span<uint8_t> payload) {
                            scanCount += 1;
                            return scanCount < scanLength;
                        });
                        local_ops_performed += 1;
                        break;
                    } catch (OLCRestartException e) {
                        continue;
                    }
                }
            }
            ops_performed += local_ops_performed;
            barrier.arrive_and_wait();
        }, i);
    }
    {
        //pre insert
        barrier.arrive_and_wait();
        if (preInsert) {
            e.setParam("op", "insert90");
            BTreeCppPerfEventBlock b(e, t, keyCount - preInsertCount);
            barrier.arrive_and_wait();
            // work
            barrier.arrive_and_wait();
        } else {
            e.setParam("op", "insert0");
            BTreeCppPerfEventBlock b(e, t, keyCount);
            barrier.arrive_and_wait();
            // work
            barrier.arrive_and_wait();
        }
        ops_performed.store(0);
        {
            e.setParam("op", "ycsb_c");
            BTreeCppPerfEventBlock b(e, t, 1);
            barrier.arrive_and_wait();
            // work
            sleep(workDuration);
            keepWorking.store(false, std::memory_order::relaxed);
            barrier.arrive_and_wait();
            keepWorking.store(true, std::memory_order::relaxed);
            b.scale = ops_performed.load(std::memory_order::relaxed);
        }
        ops_performed.store(0);
        {
            e.setParam("op", "ycsb_e");
            BTreeCppPerfEventBlock b(e, t, 1);
            barrier.arrive_and_wait();
            //work
            sleep(workDuration);
            keepWorking.store(false, std::memory_order::relaxed);
            barrier.arrive_and_wait();
            keepWorking.store(true, std::memory_order::relaxed);
            b.scale = ops_performed.load(std::memory_order::relaxed);
        }
        ops_performed.store(0);
    }

    // Wait for all threads to complete
    for (auto &thread: threads) {
        thread.join();
    }
}


static void runContention(
        BTreeCppPerfEvent e,
        Key *data,
        unsigned keyCount,
        unsigned payloadSize,
        unsigned workDuration,
        double zipfParameter,
        unsigned threadCount
) {
    constexpr unsigned index_samples = 1 << 25;
    uint32_t *zipfIndices = generate_zipf_indices(ZIPFC_RNG, keyCount, zipfParameter, index_samples);

    uint8_t *payloadPtr = makePayload(payloadSize);
    std::span payload{payloadPtr, payloadSize};
    DataStructureWrapper t(isDataInt(e));
    std::atomic_bool keepWorking = true;
    std::atomic<uint64_t> ops_performed = 0;


    setVmcacheWorkerThreadId(threadCount);
    std::vector<std::thread> threads;
    std::barrier barrier{threadCount + 1};
    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            setVmcacheWorkerThreadId(tid);
            for (uint64_t i = rangeStart(0, keyCount, threadCount, tid);
                 i < rangeStart(0, keyCount, threadCount, tid + 1); i++) {
                t.insert(data[i].span(), payload);
            }
            unsigned threadIndexOffset = index_samples / threadCount * tid;
            unsigned local_ops_performed = 0;
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            while (keepWorking.load(std::memory_order::relaxed)) {
                unsigned zipf_index = threadIndexOffset + local_ops_performed * 2;
                unsigned zipf1 = zipfIndices[zipf_index % index_samples];
                unsigned zipf2 = zipfIndices[(zipf_index + 1) % index_samples];
                unsigned keyIndex = zipf1 + zipf2 % 64;
                if (keyIndex >= keyCount)
                    keyIndex -= keyCount;
                t.insert(data[keyIndex].span(), payload);
                local_ops_performed += 1;
            }
            ops_performed += local_ops_performed;
            local_ops_performed = 0;
            barrier.arrive_and_wait();
        }, i);
    }
    {
        barrier.arrive_and_wait();
        {
            e.setParam("op", "update_contention");
            BTreeCppPerfEventBlock b(e, t, 1);
            barrier.arrive_and_wait();
            // work
            sleep(workDuration);
            keepWorking.store(false, std::memory_order::relaxed);
            barrier.arrive_and_wait();
            b.scale = ops_performed.load(std::memory_order::relaxed);
        }
    }

    // Wait for all threads to complete
    for (auto &thread: threads) {
        thread.join();
    }
}


static void runMultiLarge(BTreeCppPerfEvent e,
                          std::function<Key(uint64_t, uint8_t *)> data,
                          uint64_t keyCount,
                          unsigned payloadSize,
                          unsigned workDuration,
                          double zipfParameter,
                          unsigned maxScanLength,
                          unsigned threadCount
) {
    u64 scanDuration = envOr("SCAN_DURATION", workDuration);
    u64 writeSleep = envOr("WRITE_SLEEP", 0);
    constexpr uint64_t index_samples = 1ull << 31;
    ZipfPermutation *zipfP = create_zipf_permutation(ZIPFC_RNG, keyCount, zipfParameter);
    uint8_t *payloadPtr = makePayload(payloadSize);
    std::span payload{payloadPtr, payloadSize};
    std::atomic_bool keepWorking = true;
    std::atomic<uint64_t> ops_performed = 0;

    DataStructureWrapper t(isDataInt(e));

    setVmcacheWorkerThreadId(threadCount);
    std::vector<std::thread> threads;
    std::barrier barrier{threadCount + 1};
    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            setVmcacheWorkerThreadId(tid);
            auto local_zipf_rng = create_zipfc_rng(0, tid, "thread_rng");
            std::array<uint32_t, 4096> zipfIndices;
            unsigned nextZipfIndex = zipfIndices.size();
            uint8_t outBuffer[maxKvSize];
            uint8_t keyBuffer[maxKvSize];
            unsigned local_ops_performed = 0;
            auto getZipfIndex = [&]() {
                if (nextZipfIndex == zipfIndices.size()) {
                    fill_zipf_single_thread(local_zipf_rng, zipfP, zipfIndices.data(), zipfIndices.size());
                    nextZipfIndex = 0;
                }
                uint32_t ret = zipfIndices[nextZipfIndex];
                nextZipfIndex += 1;
                return ret;
            };
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            //insert
            for (uint64_t i = rangeStart(0, keyCount, threadCount, tid);
                 i < rangeStart(0, keyCount, threadCount, tid + 1); i++) {
                Key key = data(i, keyBuffer);
                t.insert(key.span(), payload);
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            // ycsb-c
            while (keepWorking.load(std::memory_order::relaxed)) {
                uint64_t keyIndex = getZipfIndex();
                assert(keyIndex < keyCount);
                if (!t.lookup(data(keyIndex, keyBuffer).span())) {
                    std::cout << "missing key " << keyIndex << std::endl;
                    abort();
                }
                local_ops_performed += 1;
            }
            ops_performed += local_ops_performed;
            barrier.arrive_and_wait();
            local_ops_performed = 0;
            std::minstd_rand local_rng(tid);
            std::uniform_int_distribution range_len_distribution(unsigned(1), maxScanLength);
            barrier.arrive_and_wait();
            // ycsb-e
            while (keepWorking.load(std::memory_order::relaxed)) {
                uint64_t index = getZipfIndex();
                Key key = data(index, keyBuffer);
                assert(index < keyCount);
                unsigned scanLength = range_len_distribution(local_rng);
                while (keepWorking.load(std::memory_order::relaxed)) {
                    unsigned scanCount = 0;
                    try {
                        t.range_lookup(key.span(), outBuffer, [&](unsigned keyLen, std::span<uint8_t> payload) {
                            scanCount += 1;
                            return scanCount < scanLength;
                        });
                        local_ops_performed += 1;
                        break;
                    } catch (OLCRestartException e) {
                        continue;
                    }
                }
            }
            ops_performed += local_ops_performed;
            barrier.arrive_and_wait();
        }, i);
    }
    {
        //pre insert
        barrier.arrive_and_wait();
        {
            e.setParam("op", "insert0");
            BTreeCppPerfEventBlock b(e, t, keyCount);
            barrier.arrive_and_wait();
            // work
            barrier.arrive_and_wait();
        }
        ops_performed.store(0);
        {
            sleep(writeSleep);
            e.setParam("op", "ycsb_c");
            BTreeCppPerfEventBlock b(e, t, 1);
            barrier.arrive_and_wait();
            // work
            sleep(workDuration);
            keepWorking.store(false, std::memory_order::relaxed);
            barrier.arrive_and_wait();
            keepWorking.store(true, std::memory_order::relaxed);
            b.scale = ops_performed.load(std::memory_order::relaxed);
        }
        ops_performed.store(0);
        {
            e.setParam("op", "ycsb_e");
            BTreeCppPerfEventBlock b(e, t, 1);
            barrier.arrive_and_wait();
            //work
            sleep(scanDuration);
            keepWorking.store(false, std::memory_order::relaxed);
            barrier.arrive_and_wait();
            keepWorking.store(true, std::memory_order::relaxed);
            b.scale = ops_performed.load(std::memory_order::relaxed);
        }
        ops_performed.store(0);
    }

    // Wait for all threads to complete
    for (auto &thread: threads) {
        thread.join();
    }
}

static void runPartitionedInsert(BTreeCppPerfEvent e,
                                 unsigned partition_count,
                                 unsigned keyCount,
                                 unsigned workDuration,
                                 unsigned payloadSize,
                                 unsigned threadCount
) {
    constexpr unsigned index_samples = 1 << 25;
    uint8_t *payloadPtr = makePayload(payloadSize);
    std::span payload{payloadPtr, payloadSize};
    std::atomic_bool keepWorking = true;
    std::atomic<uint64_t> ops_performed = 0;
    DataStructureWrapper t(isDataInt(e));
    setVmcacheWorkerThreadId(threadCount);
    std::vector<std::thread> threads;
    std::barrier barrier{threadCount + 1};

    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            union {
                uint8_t b[8];
                struct {
                    uint32_t tid;
                    uint32_t counter;
                };
            } nextKey;
            nextKey.tid = tid;
            uint32_t local_ops_performed = 0;
            setVmcacheWorkerThreadId(tid);
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            //insert
            for (; keepWorking.load(std::memory_order::relaxed); ++local_ops_performed) {
                nextKey.counter = __builtin_bswap32(local_ops_performed);
                t.insert({nextKey.b, 8}, payload);
            }
            ops_performed += local_ops_performed;
            barrier.arrive_and_wait();
        }, i);
    }

    {
        {
            barrier.arrive_and_wait();
            e.setParam("op", "insert_thread_partition");
            BTreeCppPerfEventBlock b(e, t, 1);
            barrier.arrive_and_wait();
            sleep(workDuration);
            keepWorking.store(false, std::memory_order::relaxed);
            // work
            barrier.arrive_and_wait();
            b.scale = ops_performed;
        }

        // Wait for all threads to complete
        for (auto &thread: threads) {
            thread.join();
        }
    }
}


static void runMixed(BTreeCppPerfEvent e,
                     Key *data,
                     unsigned keyCount,
                     unsigned payloadSize,
                     unsigned workDuration,
                     double zipfParameter,
                     unsigned maxScanLength,
                     unsigned threadCount,
                     unsigned insertShare,
                     unsigned lookupShare,
                     unsigned rangeShare
) {
    if (insertShare + lookupShare + rangeShare != 8) {
        std::cerr << "workload mix does not add up to 8" << std::endl;
        abort();
    }
    constexpr unsigned index_samples = 1 << 25;
    uint32_t *zipfIndices = generate_zipf_indices(ZIPFC_RNG, keyCount, zipfParameter, index_samples);
    uint8_t *payloadPtr = makePayload(payloadSize);
    std::span payload{payloadPtr, payloadSize};
    unsigned preInsertCount = keyCount - keyCount / 10;
    std::atomic_bool keepWorking = true;
    std::atomic<uint64_t> ops_performed = 0;

    const unsigned insertThreshold = insertShare * 256 / 8;
    const unsigned scanThreshold = (insertShare + rangeShare) * 256 / 8;

    DataStructureWrapper t(isDataInt(e));

    setVmcacheWorkerThreadId(threadCount);
    std::vector<std::thread> threads;
    std::barrier barrier{threadCount + 1};
    // for collecting node counts
    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            setVmcacheWorkerThreadId(tid);
            uint8_t outBuffer[maxKvSize];
            unsigned threadIndexOffset = index_samples / threadCount * tid;
            unsigned local_ops_performed = 0;
            t.start_batch();
            for (uint64_t i = rangeStart(0, preInsertCount, threadCount, tid);
                 i < rangeStart(0, preInsertCount, threadCount, tid + 1); i++) {
                t.insert(data[i].span(), payload);
            }
            t.end_batch();
            std::minstd_rand local_rng(tid);
            std::uniform_int_distribution range_len_distribution(unsigned(1), maxScanLength);
            auto rng = create_zipfc_rng(0, tid, "thread_rng");
            std::array<uint8_t, 1 << 12> ops;
            fill_u64_single_thread(rng, reinterpret_cast<uint64_t *>(ops.data()), ops.size() / 8);
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            t.start_batch();
            while (keepWorking.load(std::memory_order::relaxed)) {
                local_ops_performed += 1;
                if (local_ops_performed % ops.size() == 0) {
                    fill_u64_single_thread(rng, reinterpret_cast<uint64_t *>(ops.data()), ops.size() / 8);
                }
                unsigned keyIndex = zipfIndices[(threadIndexOffset + local_ops_performed) % index_samples];
                uint8_t op_type = ops[local_ops_performed % ops.size()];
                if (op_type < insertThreshold) {
                    t.insert(data[keyIndex].span(), payload);
                } else if (op_type < scanThreshold) {
                    //scan
                    unsigned scanLength = range_len_distribution(local_rng);
                    while (keepWorking.load(std::memory_order::relaxed)) {
                        unsigned scanCount = 0;
                        try {
                            t.range_lookup(data[keyIndex].span(), outBuffer,
                                           [&](unsigned keyLen, std::span<uint8_t> payload) {
                                               scanCount += 1;
                                               return scanCount < scanLength;
                                           });
                            break;
                        } catch (OLCRestartException e) {
                            continue;
                        }
                    }
                } else {
                    //lookup
                    while (keepWorking.load(std::memory_order::relaxed)) {
                        try {
                            unsigned len = payloadSize;
                            t.lookup(data[keyIndex].span(), [&](auto val) { len = val.size(); });
                            if (len != payloadSize)
                                abort();
                            break;
                        } catch (OLCRestartException e) {
                            continue;
                        }
                    }
                }
            }
            ops_performed += local_ops_performed - 1;
            t.end_batch();
            barrier.arrive_and_wait();
        }, i);
    }
    {
        barrier.arrive_and_wait();
        {
            std::stringstream op_name;
            op_name << "mixed" << insertShare << lookupShare << rangeShare;
            e.setParam("op", op_name.str());
            BTreeCppPerfEventBlock b(e, t, keyCount - preInsertCount);
            barrier.arrive_and_wait();
            sleep(workDuration);
            keepWorking.store(false, std::memory_order::relaxed);
            // work
            barrier.arrive_and_wait();
            b.scale = ops_performed;
        }
    }

    // Wait for all threads to complete
    for (auto &thread: threads) {
        thread.join();
    }
}

void ensure(bool x) {
    if (!x)
        abort();
}

void runTest(unsigned int threadCount, unsigned int keyCount, unsigned int seed) {
    constexpr static uint32_t WRITE_BIT = 1 << 31;
    constexpr static uint32_t BATCH_MASK = WRITE_BIT - 1;
    constexpr static uint32_t SCAN_LEN = 10;

    std::cout << "seed: " << seed << std::endl;
    Key *data = zipfc_load_keys(ZIPFC_RNG, "test", keyCount, 1.0, 1);
    auto keyState = new std::atomic<uint32_t>[keyCount];
    for (int i = 0; i < keyCount; ++i)
        std::atomic_init(keyState + i, 0);
    std::uniform_int_distribution op_distr((uint32_t) 0, (uint32_t) 9);
    std::uniform_int_distribution key_index_distr((uint32_t) 0, (uint32_t) keyCount - 1);

    uint32_t ops_per_batch = keyCount / threadCount;
    constexpr static uint32_t batch_count = 1000;

    DataStructureWrapper tree(false);

    std::vector<std::thread> threads;
    std::barrier barrier{threadCount};
    for (int tid_outer = 0; tid_outer < threadCount; ++tid_outer) {
        threads.emplace_back([&](unsigned tid) {
            for (uint32_t batch = 1; batch < batch_count + 1; ++batch) {
                if (tid == 0) {
                    std::cout << "batch: " << batch << std::endl;
                }
                union {
                    uint32_t i;
                    uint8_t b[4];
                } batch_convert;
                uint8_t outBuffer[maxKvSize];
                std::span<uint8_t> outSpan{outBuffer, 0};
                batch_convert.i = batch;

                unsigned distinct_write_count = 0;
                unsigned written_count = 0;
                unsigned write_count = 0;
                unsigned read_count = 0;

                for (uint32_t phase = 0; phase <= 1; ++phase) {
                    barrier.arrive_and_wait();
                    if (phase == 1)
                        tree.start_batch();
                    // minstd_rand is not sufficient here, there is much less overlap between reads and writes than would be expected.
                    std::mt19937 local_rng(tid + batch * threadCount);
                    for (uint32_t op = 0; op < ops_per_batch; ++op) {
                        uint32_t key_index = key_index_distr(local_rng);
                        uint32_t op_type = op_distr(local_rng);
                        if (op_type == 0) {
                            if (phase == 0) {
                                write_count += 1;
                                auto old_val = keyState[key_index].fetch_or(WRITE_BIT, std::memory_order_relaxed);
                                if ((old_val & WRITE_BIT) == 0) {
                                    distinct_write_count += 1;
                                }
                            } else {
                                batch_convert.i = batch;
                                tree.insert(data[key_index].span(), {batch_convert.b, 4});
                            }
                        } else if (op_type == 1) {
                            if (phase == 1)
                                while (true)
                                    try {
                                        unsigned returned_count = 0;
                                        unsigned returned_key = key_index;
                                        bool error = false;
                                        tree.range_lookup(data[key_index].span(), outBuffer,
                                                          [&](unsigned keyLen, std::span<uint8_t> payload) {
                                                              returned_count += 1;
                                                              ensure(returned_count <= SCAN_LEN);
                                                              while (!error) {
                                                                  auto cmp = span_compare({outBuffer, keyLen},
                                                                                          data[returned_key].span());
                                                                  if (cmp < 0) {
                                                                      error = true;
                                                                  } else if (cmp == 0) {
                                                                      break;
                                                                  } else {
                                                                      if (returned_key + 1 < keyCount &&
                                                                              ((keyState[returned_key] & BATCH_MASK) ==
                                                                               0)) {
                                                                          returned_key += 1;
                                                                      } else {
                                                                          error = true;
                                                                      }
                                                                  }
                                                              }
                                                              error |= keyState[returned_key] == 0;
                                                              error |= payload.size() != 4;
                                                              if (!error) {
                                                                  copySpan({batch_convert.b, 4}, payload);
                                                                  bool val_ok =
                                                                          ((keyState[returned_key] & BATCH_MASK) ==
                                                                           batch_convert.i) ||
                                                                                  (keyState[returned_key] &
                                                                                   WRITE_BIT) &&
                                                                                  batch_convert.i == batch;
                                                                  error |= !val_ok;
                                                              }
                                                              returned_key += 1;
                                                              return returned_count < SCAN_LEN;
                                                          });
                                        ensure(!error);
                                        break;
                                    } catch (OLCRestartException) {
                                        continue;
                                    }
                        } else {
                            if (phase == 1) {
                                read_count += 1;
                                while (true) {
                                    bool found = false;
                                    try {
                                        tree.lookup(data[key_index].span(), [&](auto val) {
                                            found = true;
                                            memcpy(outBuffer, val.data(), val.size());
                                            outSpan = {outBuffer, val.size()};
                                        });
                                    } catch (OLCRestartException) {
                                        continue;
                                    }
                                    auto state = keyState[key_index].load(std::memory_order_relaxed);
                                    auto expected_version = state & BATCH_MASK;
                                    auto is_written = (state & WRITE_BIT) != 0;
                                    written_count += is_written;
                                    if (found) {
                                        ensure(expected_version != 0 || is_written);
                                        ensure(outSpan.size() == 4);
                                        copySpan({batch_convert.b, 4}, outSpan);
                                        ensure(batch_convert.i == expected_version ||
                                               batch_convert.i == batch && is_written);
                                    } else {
                                        ensure(expected_version == 0);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    if (phase == 1)
                        tree.end_batch();
                }
                barrier.arrive_and_wait();
                // this is lower than it should be
                //std::cout << written_count << ' ' << write_count << ' ' << distinct_write_count << " " << read_count << std::endl;
                for (int i = rangeStart(0, keyCount, threadCount, tid);
                     i < rangeStart(0, keyCount, threadCount, tid + 1); ++i) {
                    if (keyState[i].load(std::memory_order_relaxed) & WRITE_BIT)
                        keyState[i].store(batch, std::memory_order_relaxed);
                }
            }
        }, tid_outer);
    }
    for (auto &thread: threads) {
        thread.join();
    }
}

void runLargeInsert(unsigned int threadCount, unsigned int duration) {
    std::atomic_bool keepWorking = true;
    std::atomic<uint64_t> ops_performed = 0;

    DataStructureWrapper t(false);

    std::vector<std::thread> threads;
    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            setVmcacheWorkerThreadId(tid);
            auto rng = create_zipfc_rng(0, tid, "thread_rng");
            std::array<uint64_t, 512> key_buffer;
            fill_u64_single_thread(rng, key_buffer.data(), 1);
            unsigned submission_threshold = key_buffer[0] % 5000 + 5000;
            unsigned local_ops = 0;
            while (keepWorking.load(std::memory_order::relaxed)) {
                fill_u64_single_thread(rng, key_buffer.data(), key_buffer.size());
                for (uint64_t &k: key_buffer) {
                    uint64_t *kptr = &k;
                    std::span<uint8_t> kspan{reinterpret_cast<uint8_t *>(kptr), 8};
                    t.insert(kspan, kspan);
                    local_ops += 1;
                    if (local_ops == submission_threshold) {
                        ops_performed.fetch_add(local_ops, std::memory_order::relaxed);
                        local_ops = 0;
                    }
                }
            }
        }, i);
    }
    {
        auto start_time = std::chrono::steady_clock::now();
        // Main loop
        for (int i = 0; i < duration * 10; ++i) {
            // Sleep for one second
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // Calculate elapsed time
            auto time = std::chrono::steady_clock::now();
            auto current_ops = ops_performed.load(std::memory_order::relaxed);
            auto elapsed_seconds =
                    std::chrono::duration_cast<std::chrono::microseconds>(time - start_time).count() * 1e-6;

            std::cout << elapsed_seconds << "," << current_ops << "," << threadCount << std::endl;
        }
        keepWorking = false;
    }
    // Wait for all threads to complete
    for (auto &thread: threads) {
        thread.join();
    }
}

int main(int argc, char *argv[]) {
    unsigned threadCount = envu64("THREADS");
    unsigned rand_seed = getenv("SEED") ? envu64("SEED") : time(NULL);
    ZIPFC_RNG = create_zipfc_rng(rand_seed, 0, "main");
#ifdef CHECK_TREE_OPS
    std::cerr << "CHECK_TREE_OPS enabled, forcing single threaded" << std::endl;
    threadCount = 1;
#endif
    uint64_t ycsb_variant = envu64("YCSB_VARIANT");
    unsigned keyCount = envu64("KEY_COUNT");
    if (ycsb_variant == 7) {
        runTest(threadCount, keyCount, rand_seed);
        return 0;
    }


    if (!getenv("DATA")) {
        std::cerr << "no keyset" << std::endl;
        abort();
    }
    const char *run_id = getenv("RUN_ID");
    if (!run_id) {
        std::cerr << "WARN: no run_id" << std::endl;
        char *timestmap = new char[64];
        sprintf(timestmap, "%lu",
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now().time_since_epoch()).count());
        run_id = timestmap;
    }
    std::string keySet = getenv("DATA");
    unsigned payloadSize = envu64("PAYLOAD_SIZE");
    unsigned duration = envu64("DURATION");
    double zipfParameter = envf64("ZIPF");
    double intDensity = envf64("DENSITY");
    uint64_t partition_count = envu64("PARTITION_COUNT");
    unsigned maxScanLength = envu64("SCAN_LENGTH");

    BTreeCppPerfEvent e = makePerfEvent(keySet, keyCount);
    e.setParam("payload_size", payloadSize);
    e.setParam("threads", threadCount);
    e.setParam("run_id", run_id);
    e.setParam("ycsb_zipf", zipfParameter);
    e.setParam("bin_name", std::string{argv[0]});
    e.setParam("density", intDensity);
    e.setParam("rand_seed", rand_seed);
    e.setParam("partition_count", partition_count);
    e.setParam("ycsb_range_len", maxScanLength);
    e.setParam("phys_gb", envOr("PHYSGB", 4));
    if (maxScanLength == 0) {
        throw;
    }
    Key *data;
    if (ycsb_variant == 8) {
        if (keyCount > 2000000000) {
            std::cerr << "too many keys, key index shuffling might overflow";
            abort();
        }
        if (keyCount % 128879 == 0) {
            std::cerr << "unlucky key count" << std::endl;
            abort();
        }
        constexpr unsigned STRING_DIV = 128;
        constexpr uint64_t SPARSE_PRIME = 1314734440756030799ull;
        unsigned data_id;
        if (keySet == "int") {
            data_id = 0;
        } else if (keySet == "rng8") {
            data_id = 1;
        } else {
            std::cerr << "loading strings" << std::endl;
            data_id = 2;
            data = zipfc_load_keys(ZIPFC_RNG, keySet.c_str(), (keyCount + STRING_DIV - 1) / STRING_DIV, intDensity,
                                   partition_count);
        }
        auto getKey = [&](uint64_t index, uint8_t *buffer) {
            if (data_id <= 1) {
                // turn dense integer into sparse
                if (data_id == 1) {
                    index = index *
                            SPARSE_PRIME; // this overflows, but 2^64 is coprime to SPARSE_PRIME, so we get decent shuffling
                }
                index = __builtin_bswap64(index);
                memcpy(buffer, &index, 8);
                return Key{buffer, 8};
            } else {
                uint8_t first_byte = index % STRING_DIV;
                uint64_t string_index = index / STRING_DIV;
                buffer[0] = first_byte;
                Key &string_key = data[string_index];
                memcpy(buffer + 1, string_key.data, string_key.len);
                return Key{buffer, string_key.len + 1};
            }
        };
        runMultiLarge(e, getKey, keyCount, payloadSize, duration, zipfParameter, maxScanLength, threadCount);
        return 0;
    }
    data = zipfc_load_keys(ZIPFC_RNG, keySet.c_str(), keyCount, intDensity, partition_count);
    for (unsigned i = 0; i < keyCount; ++i) {
        if (data[i].len + payloadSize > maxKvSize) {
            std::cerr << "key too long for page size" << std::endl;
            abort();
        }
    }

    if (ycsb_variant >= 6000 && ycsb_variant < 7000) {
        runMixed(e, data, keyCount, payloadSize, duration, zipfParameter, maxScanLength, threadCount,
                 ycsb_variant / 100 % 10, ycsb_variant / 10 % 10, ycsb_variant % 10);
    } else
        switch (ycsb_variant) {
            case 401: {
                abort();
            }
            case 402: {
                runLargeInsert(threadCount, duration);
                break;
            }
            case 403: {
                runPartitionedInsert(e, keyCount, partition_count, duration, payloadSize, threadCount);
                break;
            }
            case 501: {
                abort();
            }
            case 6: {
                runMulti(e, data, keyCount, payloadSize, duration, zipfParameter, maxScanLength, threadCount, false);
                break;
            }
            case 601: {
                runMixed(e, data, keyCount, payloadSize, duration, zipfParameter, maxScanLength, threadCount, 1, 6,
                         1);
                break;
            }
            case 602: {
                runMulti(e, data, keyCount, payloadSize, duration, zipfParameter, maxScanLength, threadCount, true);
                break;
            }
            case 900: {
                runContention(e, data, keyCount, payloadSize, duration, zipfParameter, threadCount);
                break;
            }
            default: {
                std::cerr << "bad ycsb variant" << std::endl;
                abort();
            }
        }

    return 0;
}