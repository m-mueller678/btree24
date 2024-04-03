#include <algorithm>
#include <csignal>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include "PerfEvent.hpp"
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
                     unsigned baseKeyCount,
                     unsigned maxKeyCount,
                     unsigned payloadSize,
                     unsigned opCountC,
                     unsigned opCountE,
                     double zipfParameter,
                     unsigned maxScanLength,
                     unsigned threadCount
) {
    uint32_t *operations_c = generate_workload_c(ZIPFC_RNG, baseKeyCount, zipfParameter, opCountC * threadCount);
    uint32_t *operations_e = generate_workload_e(ZIPFC_RNG, zipfParameter, baseKeyCount, maxKeyCount,
                                                 opCountE * threadCount);

    uint8_t *payload = makePayload(payloadSize);
    unsigned preInsertCount = baseKeyCount - baseKeyCount / 10;

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
                uint8_t *key = (uint8_t *) data[i].data;
                unsigned int length = data[i].len;
                // TODO t.insert(key, length, payload, payloadSize);
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            //insert
            for (uint64_t i = rangeStart(preInsertCount, baseKeyCount, threadCount, tid);
                 i < rangeStart(preInsertCount, baseKeyCount, threadCount, tid + 1); i++) {
                uint8_t *key = (uint8_t *) data[i].data;
                unsigned int length = data[i].len;
                // TODO t.insert(key, length, payload, payloadSize);
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            // ycsb-c
            for (uint64_t i = 0; i < opCountC; i++) {
                unsigned keyIndex = operations_c[tid * opCountC + i];
                assert(keyIndex < baseKeyCount);
                unsigned payloadSizeOut;
                uint8_t *key = (uint8_t *) data[keyIndex].data;
                unsigned long length = data[keyIndex].len;
                //TODO uint8_t *payload = t.lookup(key, length, payloadSizeOut,);
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            // ycsb-e
            for (uint64_t i = 0; i < opCountE; i++) {
                unsigned op = operations_e[tid * opCountC + i];
                unsigned index = op & ((uint32_t(1) << 31) - 1);
                bool is_insert = (op >> 31) != 0;
                assert(index < maxKeyCount);
                //TODO
            }
            barrier.arrive_and_wait();
        }, i);
    }
    {
        barrier.arrive_and_wait();
        // pre insert
        {
            barrier.arrive_and_wait();
            e.setParam("op", "insert90");
            BTreeCppPerfEventBlock b(e, t, baseKeyCount - preInsertCount);
            barrier.arrive_and_wait();
        }
        {
            barrier.arrive_and_wait();
            e.setParam("op", "ycsb_c");
            BTreeCppPerfEventBlock b(e, t, opCountC);
            barrier.arrive_and_wait();
        }
        {
            barrier.arrive_and_wait();
            e.setParam("op", "ycsb_e");
            BTreeCppPerfEventBlock b(e, t, opCountE);
            barrier.arrive_and_wait();
        }
    }

    // Wait for all threads to complete
    for (auto &thread: threads) {
        thread.join();
    }
}

static unsigned workloadGenCount(unsigned keyCount, unsigned opCount, unsigned ycsbVariant) {
    switch (envu64("YCSB_VARIANT")) {
        case 401: {
            return keyCount;
        }
        case 402: {
            return keyCount;
        }
        case 501: {
            return keyCount;
        }
        case 6: {
            return keyCount;
        }
        default: {
            std::cerr << "bad ycsb variant" << std::endl;
            abort();
        }
    }
}

int main(int argc, char *argv[]) {
    bool dryRun = getenv("DRYRUN");

    unsigned rand_seed = getenv("SEED") ? envu64("SEED") : time(NULL);
    ZIPFC_RNG = create_zipfc_rng(rand_seed, 0, "main");

    unsigned keyCount = envu64("KEY_COUNT");
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
    unsigned threadCount = envu64("THREADS");
    double zipfParameter = envf64("ZIPF");
    double intDensity = envf64("DENSITY");
    uint64_t ycsb_variant = envu64("YCSB_VARIANT");
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

    auto keyGenCount = workloadGenCount(keyCount, opCount, ycsb_variant);
    Key *data = zipfc_load_keys(ZIPFC_RNG, keySet.c_str(), keyCount, intDensity, partition_count);
    for (unsigned i = 0; i < keyGenCount; ++i) {
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
            runMulti(e, data, keyCount, keyGenCount, payloadSize, opCount, opCount, zipfParameter, maxScanLength,
                     threadCount);
            break;
        }
        default: {
            std::cerr << "bad ycsb variant" << std::endl;
            abort();
        }
    }

    return 0;
}
