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

    bool lookup(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    static uint8_t compute_hash(std::span<uint8_t> key);

    uint8_t *ptr();

    std::span<uint8_t> hashes();

    std::span<uint8_t> slice(uint16_t offset, uint16_t len);

    std::span<uint8_t> getKey(unsigned slotId);

    std::span<uint8_t> getPayload(unsigned slotId);

    void
    init(std::span<uint8_t> lowerFence, std::span<uint8_t> upperFence, unsigned int hashCapacity, RangeOpCounter roc);

    static GuardX<AnyNode> makeRootLeaf();

    std::span<uint8_t> getLowerFence();

    std::span<uint8_t> getUpperFence();

    void updatePrefixLength();

    bool insert(std::span<uint8_t> key, std::span<uint8_t> payload);

    int findIndex(std::span<uint8_t> key, uint8_t hash);

    unsigned int freeSpace();

    unsigned int freeSpaceAfterCompaction();

    bool requestSpace(unsigned int spaceNeeded);

    bool requestSlotAndSpace(unsigned int spaceNeeded);

    void compactify(unsigned newHashCapacity);

    bool isSorted();
    void sort();

    unsigned int commonPrefix(unsigned int slotA, unsigned int slotB);

    SeparatorInfo findSeparator();

    void getSep(uint8_t *sepKeyOut, SeparatorInfo info);

    void splitNode(AnyNode *parent, unsigned int sepSlot, std::span<uint8_t> sepKey);

    AnyNode *any() { return reinterpret_cast<AnyNode *>(this); }

    void updateHash(unsigned int i);

    void copyKeyValue(unsigned srcSlot, HashNode *dst, unsigned dstSlot);

    void storeKeyValue(unsigned int slotId, std::span<uint8_t> key, std::span<uint8_t> payload, uint8_t hash);

    void copyKeyValueRange(HashNode *dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);

    bool removeSlot(unsigned int slotId);

    bool remove(uint8_t *key, unsigned int keyLength);

    bool mergeNodes(unsigned int slotId, AnyNode *parent, HashNode *right);

    void print();

    bool range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);

    unsigned int lowerBound(std::span<uint8_t> key, bool &found);

    int findIndexNoSimd(std::span<uint8_t> key, uint8_t hash);

    int findIndexSimd(std::span<uint8_t> key, uint8_t hash);

    void validate();

    void copyKeyValueRangeToBasic(BTreeNode *dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);

    void copyKeyValueToBasic(unsigned int srcSlot, BTreeNode *dst, unsigned int dstSlot);

    void splitToBasic(AnyNode *parent, unsigned int sepSlot, std::span<uint8_t> sepKey);

    bool tryConvertToBasic();

    bool hasGoodHeads();
} __attribute__((aligned(hashSimdWidth)));

#endif //BTREE24_HASHNODE_HPP
