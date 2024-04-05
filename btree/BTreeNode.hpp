#ifndef BTREE24_BTREENODE_HPP
#define BTREE24_BTREENODE_HPP


#include "Tag.hpp"
#include "vmache.hpp"
#include "nodes.hpp"
#include "SeparatorInfo.hpp"

struct BTreeNodeHeader : public TagAndDirty {
    static constexpr unsigned hintCount = basicHintCount;
    static constexpr unsigned underFullSizeLeaf = pageSizeLeaf / 4;    // merge nodes below this size
    static constexpr unsigned underFullSizeInner = pageSizeInner / 4;  // merge nodes below this size

    uint16_t count = 0;
    uint16_t spaceUsed = 0;
    uint16_t dataOffset;

    struct FenceKeySlot {
        uint16_t offset;
        uint16_t length;
    };

    PID upper = 0;  // only used in inner nodes

    FenceKeySlot lowerFence = {0, 0};  // exclusive
    FenceKeySlot upperFence = {0, 0};  // inclusive

    uint16_t prefixLength = 0;

    uint32_t hint[hintCount];

    void init(bool isLeaf, RangeOpCounter roc);
};

struct BTreeNode : public BTreeNodeHeader {
    struct Slot {
        uint16_t offset;
        uint16_t keyLen;
        uint16_t payloadLen;
        union {
            uint32_t head[enableBasicHead];
            uint8_t headBytes[enableBasicHead ? 4 : 0];
        };
    } __attribute__((packed));
    union {
        Slot slot[1];     // grows from front
        uint8_t heap[1];  // grows from back
    };

    // this struct does not have appropriate size.
    // Get Some storage location and call init.
    BTreeNode() = delete;

    static constexpr unsigned maxKVSize =
            (((pageSizeLeaf < pageSizeInner ? pageSizeLeaf : pageSizeInner) - sizeof(BTreeNodeHeader) -
              (2 * sizeof(Slot)))) / 3;

    void init(bool isLeaf, RangeOpCounter roc);

    uint8_t *ptr();

    bool isInner();

    bool isLeaf();

    std::span<uint8_t> getLowerFence();

    std::span<uint8_t> getUpperFence();

    std::span<uint8_t> getPrefix();

    unsigned freeSpace();

    unsigned freeSpaceAfterCompaction();

    bool requestSpaceFor(unsigned spaceNeeded);

    static GuardX<AnyNode> makeLeaf();

    std::span<uint8_t> getKey(unsigned slotId);

    std::span<uint8_t> getPayload(unsigned slotId);

    PID getChild(unsigned slotId);

    // How much space would inserting a new key of length "getKeyLength" require?
    unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength);

    void makeHint();

    void updateHint(unsigned slotId);

    void searchHint(uint32_t keyHead, unsigned &lowerOut, unsigned &upperOut);

    // lower bound search, foundOut indicates if there is an exact match, returns slotId
    unsigned lowerBound(std::span<uint8_t> key, bool &foundOut);

    // lowerBound wrapper ignoring exact match argument (for convenience)
    unsigned lowerBound(std::span<uint8_t> key);

    bool insert(std::span<uint8_t> key, std::span<uint8_t> payload);

    void removeSlot(unsigned slotId);

    bool remove(std::span<uint8_t> key);

    void compactify();

    // merge "this" into "right" via "tmp"
    bool mergeNodes(unsigned slotId, AnyNode *parent, BTreeNode *right);

    // store key/value pair at slotId
    void storeKeyValue(uint16_t slotId, std::span<uint8_t> key, std::span<uint8_t> payload);

    void copyKeyValueRange(BTreeNode *dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount);

    void copyKeyValue(uint16_t srcSlot, BTreeNode *dst, uint16_t dstSlot);

    void insertFence(FenceKeySlot &slot, std::span<uint8_t> fence);

    void setFences(std::span<uint8_t> lower, std::span<uint8_t> upper);

    void splitNode(AnyNode *parent, unsigned sepSlot, uint8_t *sepKey, unsigned sepLength);

    unsigned commonPrefix(unsigned slotA, unsigned slotB);

    SeparatorInfo findSeparator();

    void getSep(uint8_t *sepKeyOut, SeparatorInfo info);

    PID lookupInner(std::span<std::uint8_t> key);

    bool lookupLeaf(std::span<uint8_t> key, std::span<uint8_t> &valueOut);

    void destroy();

    AnyNode *any() { return reinterpret_cast<AnyNode *>(this); }

    bool is_underfull();

    bool insertChild(std::span<uint8_t> key, PID child);

    bool range_lookup(uint8_t *key,
                      unsigned int keyLen,
                      uint8_t *keyOut,
                      const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    void validate_child_fences();

    void print();

    void restoreKeyExclusive(std::span<uint8_t> keyOut, unsigned index);

    void validateHint();

    bool range_lookup_desc(uint8_t *key,
                           unsigned int keyLen,
                           uint8_t *keyOut,
                           const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    bool hasBadHeads();

    void splitToHash(AnyNode *parent, unsigned int sepSlot, uint8_t *sepKey, unsigned int sepLength);

    void copyKeyValueRangeToHash(HashNode *dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);

    bool tryConvertToHash();

    std::span<uint8_t> slice(uint16_t offset, uint16_t len);
};

union TmpBTreeNode {
    BTreeNode node;
    uint8_t _bytes[std::max(pageSizeLeaf, pageSizeInner)];

    TmpBTreeNode() {}
};

#endif //BTREE24_BTREENODE_HPP
