#ifndef BTREE24_DATASTRUCTUREWRAPPER_HPP
#define BTREE24_DATASTRUCTUREWRAPPER_HPP

#include "config.hpp"
#include "BTree.hpp"
#include <map>

struct DataStructureWrapper {
    DataStructureWrapper(bool isInt);

#ifdef CHECK_TREE_OPS
    std::map<std::vector<uint8_t>, std::vector<uint8_t>> std_map;
#endif
#if defined(USE_STRUCTURE_BTREE)
    BTree impl;
#elif defined(USE_STRUCTURE_ART)
    ArtBTreeAdapter impl;
#elif defined(USE_STRUCTURE_HOT)
    HotBTreeAdapter impl;
#elif defined(USE_STRUCTURE_WH)
    WhBTreeAdapter impl;
#elif defined(USE_STRUCTURE_TLX)
    TlxWrapper impl;
#endif

    uint8_t *lookup(uint8_t *key, unsigned keyLength, unsigned &payloadSizeOut);

    bool lookup(uint8_t *key, unsigned keyLength) {
        unsigned ignore;
        return lookup(key, keyLength, ignore) != nullptr;
    }

    void insert(std::span<uint8_t> key, std::span<uint8_t> payload);

    bool remove(uint8_t *key, unsigned keyLength);

    void range_lookup(uint8_t *key,
                      unsigned int keyLen,
                      uint8_t *keyOut,
                      const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    void range_lookup_desc(uint8_t *key,
                           unsigned int keyLen,
                           uint8_t *keyOut,
                           const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    void testing_update_payload(uint8_t *key, unsigned int keyLength, uint8_t *payload);
};

#endif //BTREE24_DATASTRUCTUREWRAPPER_HPP
