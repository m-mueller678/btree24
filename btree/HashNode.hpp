#ifndef BTREE24_HASHNODE_HPP
#define BTREE24_HASHNODE_HPP

#include "Tag.hpp"
#include "nodes.hpp"
#include "vmache.hpp"
#include "SeparatorInfo.hpp"


struct HashNodeHeader : public TagAndDirty {
    RangeOpCounter rangeOpCounter;
    uint16_t count;
    uint16_t sortedCount;
    // includes keys, payloads, and fences
    // excludes slots and hash array
    uint16_t spaceUsed;
    uint16_t dataOffset;
    uint16_t prefixLength;
    uint16_t hashCapacity;
    uint16_t hashOffset;
    uint16_t lowerFenceLen;
    uint16_t upperFenceLen;
};


struct HashSlot {
    uint16_t offset;
    uint16_t keyLen;
    uint16_t payloadLen;
};

struct HashNode : public HashNodeHeader {
    union {
        HashSlot slot[(pageSizeLeaf - sizeof(HashNodeHeader)) / sizeof(HashSlot)];  // grows from front
        uint8_t heap[pageSizeLeaf - sizeof(HashNodeHeader)];                        // grows from back
    };

    unsigned estimateCapacity();

    uint8_t *lookup(uint8_t *key, unsigned keyLength, unsigned &payloadSizeOut);

    static uint8_t compute_hash(uint8_t *key, unsigned int keyLength);

    uint8_t *ptr();

    uint8_t *hashes();

    uint8_t *getPayload(unsigned int slotId);

    uint8_t *getKey(unsigned int slotId);

    void init(uint8_t *lowerFence,
              unsigned int lowerFenceLen,
              uint8_t *upperFence,
              unsigned int upperFenceLen,
              unsigned hashCapacity,
              RangeOpCounter roc);

    static GuardX<AnyNode> makeRootLeaf();

    uint8_t *getLowerFence();

    uint8_t *getUpperFence();

    void updatePrefixLength();

    bool insert(uint8_t *key, unsigned int keyLength, uint8_t *payload, unsigned int payloadLength);

    int findIndex(uint8_t *key, unsigned keyLength, uint8_t hash);

    unsigned int freeSpace();

    unsigned int freeSpaceAfterCompaction();

    bool requestSpace(unsigned int spaceNeeded);

    bool requestSlotAndSpace(unsigned int spaceNeeded);

    void compactify(unsigned newHashCapacity);

    void sort();

    unsigned int commonPrefix(unsigned int slotA, unsigned int slotB);

    SeparatorInfo findSeparator();

    void getSep(uint8_t *sepKeyOut, SeparatorInfo info);

    void splitNode(AnyNode *parent, unsigned int sepSlot, uint8_t *sepKey, unsigned int sepLength);

    AnyNode *any() { return reinterpret_cast<AnyNode *>(this); }

    void updateHash(unsigned int i);

    void copyKeyValue(unsigned srcSlot, HashNode *dst, unsigned dstSlot);

    void storeKeyValue(unsigned int slotId, uint8_t *key, unsigned int keyLength, uint8_t *payload,
                       unsigned int payloadLength, uint8_t hash);

    void copyKeyValueRange(HashNode *dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);

    bool removeSlot(unsigned int slotId);

    bool remove(uint8_t *key, unsigned int keyLength);

    bool mergeNodes(unsigned int slotId, AnyNode *parent, HashNode *right);

    void print();

    bool range_lookup(uint8_t *key,
                      unsigned int keyLen,
                      uint8_t *keyOut,
                      const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    unsigned int lowerBound(uint8_t *key, unsigned int keyLength, bool &found);

    int findIndexNoSimd(uint8_t *key, unsigned keyLength, uint8_t hash);

    int findIndexSimd(uint8_t *key, unsigned keyLength, uint8_t hash);

    bool range_lookup_desc(uint8_t *key,
                           unsigned int keyLen,
                           uint8_t *keyOut,
                           const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    void validate();

    void copyKeyValueRangeToBasic(BTreeNode *dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);

    void copyKeyValueToBasic(unsigned int srcSlot, BTreeNode *dst, unsigned int dstSlot);

    void splitToBasic(AnyNode *parent, unsigned int sepSlot, uint8_t *sepKey, unsigned int sepLength);

    bool tryConvertToBasic();

    bool hasGoodHeads();
} __attribute__((aligned(hashSimdWidth)));

#endif //BTREE24_HASHNODE_HPP
