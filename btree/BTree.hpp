#ifndef BTREE24_BTREE_HPP
#define BTREE24_BTREE_HPP

#include <cstdint>
#include <functional>
#include "vmache.hpp"

struct BTree {
    BTree(bool isInt);

    ~BTree();

    PID metadata_pid;

    bool lookupImpl(uint8_t *key, unsigned keyLength, unsigned &payloadSizeOut, uint8_t *payloadOut);

    void insertImpl(uint8_t *key, unsigned keyLength, uint8_t *payload, unsigned payloadLength);

    bool removeImpl(uint8_t *key, unsigned int keyLength) const;

    void range_lookupImpl(uint8_t *key,
                          unsigned int keyLen,
                          uint8_t *keyOut,
                          const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    void range_lookup_descImpl(uint8_t *key,
                               unsigned int keyLen,
                               uint8_t *keyOut,
                               const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    void splitNode(Page node, Page *parent, uint8_t *key, unsigned keyLength);

    void ensureSpace(Page *toSplit, uint8_t *key, unsigned keyLength);
};


#endif //BTREE24_BTREE_HPP
