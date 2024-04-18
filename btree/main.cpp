#include <algorithm>
#include <csignal>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include "PerfEvent.hpp"
#include "common.hpp"
#include <iostream>

#include <zipfc.h>
#include <barrier>

using namespace std;

static ZipfcRng *ZIPFC_RNG = nullptr;

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

static unsigned rangeStart(unsigned start, unsigned end, unsigned nthread, unsigned tid) {
    return start + (end - start) * tid / nthread;
}

static void runMulti(BTreeCppPerfEvent e,
                     Key *data,
                     unsigned keyCount,
                     unsigned payloadSize,
                     unsigned opCountC,
                     unsigned opCountE,
                     double zipfParameter,
                     unsigned maxScanLength,
                     unsigned threadCount
) {
    uint32_t *operationsC = generate_workload_c(ZIPFC_RNG, keyCount, zipfParameter, opCountC * threadCount);
    uint32_t *operationsE = generate_workload_c(ZIPFC_RNG, keyCount, zipfParameter, opCountE * threadCount);

    uint8_t *payloadPtr = makePayload(payloadSize);
    std::span payload{payloadPtr, payloadSize};
    unsigned preInsertCount = keyCount - keyCount / 10;

    DataStructureWrapper t(isDataInt(e));

    std::vector<std::thread> threads;
    std::barrier barrier{threadCount + 1};
    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            uint8_t outBuffer[maxKvSize];
            barrier.arrive_and_wait();
            for (uint64_t i = rangeStart(0, preInsertCount, threadCount, tid);
                 i < rangeStart(0, preInsertCount, threadCount, tid + 1); i++) {
                t.insert(data[i].span(), payload);
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            //insert
            for (uint64_t i = rangeStart(preInsertCount, keyCount, threadCount, tid);
                 i < rangeStart(preInsertCount, keyCount, threadCount, tid + 1); i++) {
                t.insert(data[i].span(), payload);
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            // ycsb-c
            for (uint64_t i = 0; i < opCountC; i++) {
                unsigned keyIndex = operationsC[tid * opCountC + i];
                assert(keyIndex < keyCount);
                if (!t.lookup(data[keyIndex].span()))
                    abort();
            }
            barrier.arrive_and_wait();
            std::minstd_rand local_rng(tid);
            std::uniform_int_distribution range_len_distribution(unsigned(1), maxScanLength);
            barrier.arrive_and_wait();
            // ycsb-e
            for (uint64_t i = 0; i < opCountE; i++) {
                unsigned index = operationsE[tid * opCountC + i];
                assert(index < keyCount);
                unsigned scanLength = range_len_distribution(local_rng);
                while (true) {
                    unsigned scanCount = 0;
                    try {
                        t.range_lookup(data[index].span(), outBuffer, [&](unsigned keyLen, std::span<uint8_t> payload) {
                            scanCount += 1;
                            return scanCount == scanLength;
                        });
                        break;
                    } catch (OLCRestartException e) {
                        continue;
                    }
                }
            }
            barrier.arrive_and_wait();
        }, i);
    }
    {
        barrier.arrive_and_wait();
        //pre insert
        barrier.arrive_and_wait();
        {
            e.setParam("op", "insert90");
            BTreeCppPerfEventBlock b(e, t, keyCount - preInsertCount);
            barrier.arrive_and_wait();
            // insert
            barrier.arrive_and_wait();
        }
        {
            e.setParam("op", "ycsb_c");
            BTreeCppPerfEventBlock b(e, t, opCountC * threadCount);
            barrier.arrive_and_wait();
            // insert
            barrier.arrive_and_wait();
        }
        {
            e.setParam("op", "ycsb_e");
            BTreeCppPerfEventBlock b(e, t, opCountE * threadCount);
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
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
                unsigned written_count = 0;
                if (tid == 0) {
                    std::cout << "batch: " << batch << std::endl;
                }
                union {
                    uint32_t batch_i;
                    uint8_t batch_b[4];
                };
                uint8_t outBuffer[maxKvSize];
                std::span<uint8_t> outSpan{outBuffer, 0};
                batch_i = batch;
                for (uint32_t phase = 0; phase <= 1; ++phase) {
                    barrier.arrive_and_wait();
                    std::minstd_rand local_rng(tid + batch * threadCount);
                    for (uint32_t op = 0; op < ops_per_batch; ++op) {
                        uint32_t key_index = key_index_distr(local_rng);
                        uint32_t op_type = op_distr(local_rng);
                        if (op_type == 0) {
                            if (phase == 0) {
                                keyState[key_index].fetch_or(WRITE_BIT, std::memory_order_relaxed);
                            } else {
                                batch_i = batch;
                                tree.insert(data[key_index].span(), {batch_b, 4});
                            }
                        } else if (op_type == 1) {
                            //TODO
                        } else {
                            if (phase == 1) {
                                bool found = tree.lookup(data[key_index].span(), outSpan);
                                auto state = keyState[key_index].load(std::memory_order_relaxed);
                                auto expected_version = state & BATCH_MASK;
                                auto is_written = (state & WRITE_BIT) != 0;
                                written_count += is_written;
                                if (found) {
                                    ensure(expected_version != 0 || is_written);
                                    ensure(outSpan.size() == 4);
                                    copySpan({batch_b, 4}, outSpan);
                                    ensure(batch_i == expected_version || batch_i == batch && is_written);
                                } else {
                                    ensure(expected_version == 0);
                                }
                            }
                        }
                    }
                }
                barrier.arrive_and_wait();
                // this is lower than it should be
                std::cout << written_count << std::endl;
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


int main(int argc, char *argv[]) {
    bool dryRun = getenv("DRYRUN");
    unsigned threadCount = envu64("THREADS");
    unsigned rand_seed = getenv("SEED") ? envu64("SEED") : time(NULL);
    ZIPFC_RNG = create_zipfc_rng(rand_seed, 0, "main");
    unsigned keyCount = envu64("KEY_COUNT");
#ifdef CHECK_TREE_OPS
    std::cerr << "CHECK_TREE_OPS enabled, forcing single threaded" << std::endl;
    threadCount = 1;
#endif
    uint64_t ycsb_variant = envu64("YCSB_VARIANT");
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
    unsigned opCount = envu64("OP_COUNT");
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
    if (maxScanLength == 0) {
        throw;
    }

    Key *data = zipfc_load_keys(ZIPFC_RNG, keySet.c_str(), keyCount, intDensity, partition_count);
    for (unsigned i = 0; i < keyCount; ++i) {
        if (data[i].len + payloadSize > maxKvSize) {
            std::cerr << "key too long for page size" << std::endl;
            abort();
        }
    }

    switch (ycsb_variant) {
        case 401: {
            abort();
        }
        case 501: {
            abort();
        }
        case 6: {
            runMulti(e, data, keyCount, payloadSize, opCount, opCount, zipfParameter, maxScanLength, threadCount);
            break;
        }
        default: {
            std::cerr << "bad ycsb variant" << std::endl;
            abort();
        }
    }

    return 0;
}
