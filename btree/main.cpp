#include <algorithm>
#include <csignal>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include "PerfEvent.hpp"
#include "btree2024.hpp"
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#include <zipfc.h>
#include <barrier>

using namespace std;

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
                     vector<string> &data,
                     unsigned keyCount,
                     unsigned payloadSize,
                     unsigned opCount,
                     double zipfParameter,
                     unsigned maxScanLength,
                     bool dryRun,
                     unsigned threadCount
) {

    if (keyCount + (opCount * threadCount) / 20 <= data.size() && keySizeAcceptable(payloadSize, data)) {
        if (!dryRun)
            random_shuffle(data.begin(), data.end());
        data.resize(keyCount);
    } else {
        std::cerr << "UNACCEPTABLE" << std::endl;
        keyCount = 0;
        opCount = 0;
    }

    uint8_t *payload = makePayload(payloadSize);
    unsigned preInsertCount = keyCount - keyCount / 10;

    DataStructureWrapper t(isDataInt(e));

    std::vector<std::thread> threads;
    std::barrier barrier{threadCount};
    for (int i = 0; i < threadCount; ++i) {
        // Start a thread and execute threadFunction with the thread ID as argument
        threads.emplace_back([&](unsigned tid) {
            barrier.arrive_and_wait();
            for (uint64_t i = rangeStart(0, preInsertCount, threadCount, tid);
                 i < rangeStart(0, preInsertCount, threadCount, tid + 1); i++) {
                uint8_t *key = (uint8_t *) data[i].data();
                unsigned int length = data[i].size();
                t.insert(key, length, payload, payloadSize);
            }
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
            //insert
            for (uint64_t i = rangeStart(preInsertCount, keyCount, threadCount, tid);
                 i < rangeStart(preInsertCount, keyCount, threadCount, tid + 1); i++) {
                uint8_t *key = (uint8_t *) data[i].data();
                unsigned int length = data[i].size();
                t.insert(key, length, payload, payloadSize);
            }
            barrier.arrive_and_wait();
            uint32_t *lookup_indices = new uint32_t[opCount];
            generate_zipf()
            barrier.arrive_and_wait();
            // lookup
            for (uint64_t i = 0; i < opCount; i++) {
                unsigned keyIndex = zipf_next(e, keyCount, zipfParameter, false, false);
                assert(keyIndex < data.size());
                unsigned payloadSizeOut;
                uint8_t *key = (uint8_t *) data[keyIndex].data();
                unsigned long length = data[keyIndex].size();
                uint8_t *payload = t.lookup(key, length, payloadSizeOut);
                if (!payload || (payloadSize != payloadSizeOut) || (payloadSize > 0 && *payload != 42))
                    throw;
            }

        }, i);
    }



    // Wait for all threads to complete
    for (auto &thread: threads) {
        thread.join();
    }



    {
        barrier.arrive_and_wait();
        // pre insert
        barrier.arrive_and_wait();
        {
            e.setParam("op", "insert90");
            BTreeCppPerfEventBlock b(e, t, keyCount - preInsertCount);
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
        }
        {
            e.setParam("op", "ycsb_c");
            BTreeCppPerfEventBlock b(e, t, opCount);
            barrier.arrive_and_wait();
            barrier.arrive_and_wait();
        }

    }

    std::minstd_rand generator(std::rand());
    std::uniform_int_distribution<unsigned> scanLengthDistribution{1, maxScanLength};

    {
        uint8_t keyBuffer[maxKvSize];
        e.setParam("op", "scan");
        BTreeCppPerfEventBlock b(e, t, opCount);
        if (!dryRun)
            for (uint64_t i = 0; i < opCount; i++) {
                unsigned scanLength = scanLengthDistribution(generator);
                unsigned keyIndex = zipf_next(e, keyCount, zipfParameter, false, false);
                assert(keyIndex < data.size());
                uint8_t *key = (uint8_t *) data[keyIndex].data();
                unsigned long keyLen = data[keyIndex].size();
                unsigned foundIndex = 0;
                auto callback = [&](unsigned keyLen, uint8_t *payload, unsigned loadedPayloadLen) {
                    if (payloadSize != loadedPayloadLen) {
                        throw;
                    }
                    foundIndex += 1;
                    return foundIndex < scanLength;
                };
                t.range_lookup(key, keyLen, keyBuffer, callback);
            }
    }

    data.clear();
}

static void
runSortedInsert(BTreeCppPerfEvent e, vector<string> &data, unsigned keyCount, unsigned payloadSize, bool dryRun,
                     bool doSort = true) {
    if (keyCount <= data.size() && keySizeAcceptable(payloadSize, data)) {
        data.resize(keyCount);
        if (!dryRun && doSort) {
            std::sort(data.begin(), data.end());
        }
    } else {
        std::cerr << "UNACCEPTABLE" << std::endl;
        keyCount = 0;
    }

    uint8_t *payload = makePayload(payloadSize);

    DataStructureWrapper t(isDataInt(e));
    {
        // insert
        e.setParam("op", "sorted_insert");
        BTreeCppPerfEventBlock b(e, t, keyCount);
        if (!dryRun)
            for (uint64_t i = 0; i < keyCount; i++) {
                uint8_t *key = (uint8_t *) data[i].data();
                unsigned int length = data[i].size();
                t.insert(key, length, payload, payloadSize);
            }
    }
    data.clear();
}

static void runSortedScan(BTreeCppPerfEvent e,
                   vector<string> &data,
                   unsigned keyCount,
                   unsigned payloadSize,
                   unsigned opCount,
                   unsigned maxScanLength,
                   double zipfParameter,
                   bool dryRun) {
    if (keyCount <= data.size()) {
        if (!dryRun)
            random_shuffle(data.begin(), data.end());
        data.resize(keyCount);
    } else {
        std::cerr << "not enough keys" << std::endl;
        keyCount = 0;
        opCount = 0;
    }

    uint8_t *payload = makePayload(payloadSize);

    DataStructureWrapper t(isDataInt(e));
    {
        // insert
        e.setParam("op", "sorted_scan_init");
        BTreeCppPerfEventBlock b(e, t, keyCount);
        if (!dryRun)
            for (uint64_t i = 0; i < keyCount; i++) {
                uint8_t *key = (uint8_t *) data[i].data();
                unsigned int length = data[i].size();
                t.insert(key, length, payload, payloadSize);
            }
    }
    uint8_t keyBuffer[maxKvSize];
    std::minstd_rand generator(std::rand());
    std::uniform_int_distribution<unsigned> scanLengthDistribution{1, maxScanLength};

    t.range_lookup(payload, 0, keyBuffer,
                   [&](unsigned keyLen, uint8_t *payload, unsigned loadedPayloadLen) { return true; });

    {
        e.setParam("op", "sorted_scan");
        BTreeCppPerfEventBlock b(e, t, opCount);
        if (!dryRun)
            for (uint64_t i = 0; i < opCount; i++) {
                unsigned keyIndex = zipf_next(e, keyCount, zipfParameter, false, false);
                assert(keyIndex < data.size());
                unsigned scanLength = scanLengthDistribution(generator);

                unsigned foundIndex = 0;
                uint8_t *key = (uint8_t *) data[keyIndex].data();
                unsigned int keyLen = data[keyIndex].size();
                auto callback = [&](unsigned keyLen, uint8_t *payload, unsigned loadedPayloadLen) {
                    if (payloadSize != loadedPayloadLen) {
                        throw;
                    }
                    foundIndex += 1;
                    return foundIndex < scanLength;
                };
                t.range_lookup(key, keyLen, keyBuffer, callback);
            }
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

    vector<string> data;

    unsigned rand_seed = getenv("SEED") ? envu64("SEED") : time(NULL);
    srand(rand_seed);
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
    BTreeCppPerfEvent e = makePerfEvent(keySet, keyCount);
    e.setParam("payload_size", payloadSize);
    e.setParam("threads", threadCount);
    e.setParam("run_id", run_id);
    e.setParam("ycsb_zipf", zipfParameter);
    e.setParam("bin_name", std::string{argv[0]});
    e.setParam("density", intDensity);
    e.setParam("rand_seed", rand_seed);
    unsigned maxScanLength = envu64("SCAN_LENGTH");
    if (maxScanLength == 0) {
        throw;
    }
    e.setParam("ycsb_range_len", maxScanLength);

    if (keySet == "int") {
        std::random_device rd;
        std::mt19937 gen(rd());
        // Create a bernoulli_distribution with the given probability
        std::bernoulli_distribution dist(intDensity);

        // Generate a random boolean value
        bool result = dist(gen);
        unsigned genCount = workloadGenCount(keyCount, opCount, envu64("YCSB_VARIANT")) / intDensity;
        vector<uint32_t> v;
        if (dryRun) {
            data.resize(genCount);
        } else {
            for (uint32_t i = 0; v.size() < genCount; i++)
                v.push_back(i);
            string s;
            s.resize(4);
            for (auto x: v) {
                *(uint32_t *) (s.data()) = __builtin_bswap32(x);
                data.push_back(s);
            }
        }
    } else if (keySet == "rng4") {
        unsigned genCount = workloadGenCount(keyCount, opCount, envu64("YCSB_VARIANT"));
        if (dryRun) {
            data.resize(genCount);
        } else {
            vector<uint32_t> v;
            v.resize(genCount);
            generate_rng4(std::rand(), genCount, v.data());
            string s;
            s.resize(4);
            data.reserve(genCount);
            for (auto x: v) {
                *(uint32_t *) (s.data()) = __builtin_bswap32(x);
                data.push_back(s);
            }
        }
    } else if (keySet == "rng8") {
        unsigned genCount = workloadGenCount(keyCount, opCount, envu64("YCSB_VARIANT"));
        if (dryRun) {
            data.resize(genCount);
        } else {
            vector<uint64_t> v;
            v.resize(genCount);
            generate_rng8(std::rand(), genCount, v.data());
            string s;
            s.resize(8);
            data.reserve(genCount);
            for (auto x: v) {
                *(uint64_t *) (s.data()) = __builtin_bswap64(x);
                data.push_back(s);
            }
        }
    } else if (keySet == "long1") {
        for (unsigned i = 0; i < keyCount; i++) {
            string s;
            for (unsigned j = 0; j < i; j++)
                s.push_back('A');
            data.push_back(s);
        }
    } else if (keySet == "long2") {
        for (unsigned i = 0; i < keyCount; i++) {
            string s;
            for (unsigned j = 0; j < i; j++)
                s.push_back('A' + random() % 60);
            data.push_back(s);
        }
    } else if (keySet == "partitioned_id") {
        unsigned partitionCount = maxScanLength;
        std::vector<uint32_t> next_id;
        for (unsigned i = 0; i < partitionCount; ++i)
            next_id.push_back(0);

        std::mt19937 gen(rand_seed);
        std::uniform_int_distribution dist(uint32_t(0), uint32_t(partitionCount - 1));

        data.reserve(keyCount);
        for (uint32_t i = 0; i < keyCount; i++) {
            uint64_t partition = dist(gen);
            uint64_t id = next_id[partition]++;
            union {
                uint64_t key;
                uint8_t keyBytes[8];
            };
            key = __builtin_bswap64(partition << 32 | id);
            data.emplace_back(keyBytes, keyBytes + 8);
        }
    } else {
        ifstream in(keySet);
        keySet = "file:" + keySet;
        if (dryRun && keySet == "file:data/access")
            data.resize(6625815);
        else if (dryRun && keySet == "file:data/genome")
            data.resize(262084);
        else if (dryRun && keySet == "file:data/urls")
            data.resize(6393703);
        else if (dryRun && keySet == "file:data/urls-short")
            data.resize(6391379);
        else if (dryRun && keySet == "file:data/wiki")
            data.resize(15772029);
        else if (dryRun) {
            std::cerr << "key count unknown for [" << keySet << "]" << std::endl;
            abort();
        } else {
            string line;
            while (getline(in, line)) {
                if (dryRun) {
                    data.emplace_back();
                } else {
                    if (configName == std::string{"art"})
                        line.push_back(0);
                    data.push_back(line);
                }
            }
        }
    }

    for (unsigned i = 0; i < data.size(); ++i) {
        if (data[i].size() + payloadSize > maxKvSize) {
            std::cerr << "key too long for page size" << std::endl;
            // this forces the key count check to fail and emits nan values.
            data.clear();
            keyCount = 1;
            break;
        }
    }

    switch (envu64("YCSB_VARIANT")) {
        case 401: {
            runSortedInsert(e, data, keyCount, payloadSize, dryRun);
            break;
        }
        case 402: {
            runSortedInsert(e, data, keyCount, payloadSize, dryRun, false);
            break;
        }
        case 501: {
            runSortedScan(e, data, keyCount, payloadSize, opCount, maxScanLength, zipfParameter, dryRun);
            break;
        }
        case 6: {
            runMulti(e, data, keyCount, payloadSize, opCount, zipfParameter, maxScanLength, dryRun, threadCount);
            break;
        }
        default: {
            std::cerr << "bad ycsb variant" << std::endl;
            abort();
        }
    }

    return 0;
}

#pragma GCC diagnostic pop