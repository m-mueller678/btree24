#ifndef BTREE24_DATASTRUCTUREWRAPPER_HPP
#define BTREE24_DATASTRUCTUREWRAPPER_HPP

#include "config.hpp"
#include "BTree.hpp"
#include "vmcache_btree.hpp"
#include "TlxWrapper.hpp"
#include "HotBTreeAdapter.hpp"
#include "WhAdapter.hpp"
#include <map>

struct DataStructureWrapper {
    DataStructureWrapper(bool isInt);

#ifdef CHECK_TREE_OPS
    std::map<std::vector<uint8_t>, std::vector<uint8_t>> std_map;
#endif
#if defined(USE_STRUCTURE_BTREE)
    BTree impl;
#elif defined(USE_STRUCTURE_VMCACHE)
    VmcBTree impl;
#elif defined(USE_STRUCTURE_ART)
    ArtBTreeAdapter impl;
#elif defined(USE_STRUCTURE_HOT)
    HotBTreeAdapter impl;
#elif defined(USE_STRUCTURE_WH)
    WhAdapter impl;
#elif defined(USE_STRUCTURE_TLX)
    TlxWrapper impl;
#endif

    // valueOut must be at least maxKvSize. The btree does not check if the value fits.
    // Due optimistic locks, large values that have never been inserted may be written to valueOut.
    void lookup(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    bool lookup(std::span<uint8_t> key) {
        while (true) {
            bool found = false;
            try {
                lookup(key, [&](auto value) { found = true; });
            } catch (OLCRestartException) {
                continue;
            }
            return found;
        }
    }

    void insert(std::span<uint8_t> key, std::span<uint8_t> payload);

    bool remove(uint8_t *key, unsigned keyLength);

    // keyOutBuffer must be at least maxKvSize.
    // may throw OLCRestartException.
    void range_lookup(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                      const std::function<bool(unsigned, std::span<uint8_t>)> &found_record_cb);

    void testing_update_payload(uint8_t *key, unsigned int keyLength, uint8_t *payload);

    void start_batch() {
#ifdef USE_STRUCTURE_WH
        impl.start_batch();
#endif
    }

    void end_batch() {
#ifdef USE_STRUCTURE_WH
        impl.end_batch();
#endif
    }
};

#endif //BTREE24_DATASTRUCTUREWRAPPER_HPP
