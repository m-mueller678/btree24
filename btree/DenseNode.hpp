#ifndef BTREE24_DENSENODE_HPP
#define BTREE24_DENSENODE_HPP


#include <cstdint>
#include <functional>
#include "Tag.hpp"
#include "nodes.hpp"
#include "functional"

typedef uint32_t NumericPart;
constexpr unsigned maxNumericPartLen = sizeof(NumericPart);

typedef uint64_t Mask;
constexpr unsigned maskBytesPerWord = sizeof(Mask);
constexpr unsigned maskBitsPerWord = 8 * maskBytesPerWord;

enum KeyError : int {
    WrongLen = -1,
    NotNumericRange = -2,
    SlightlyTooLarge = -3,
    FarTooLarge = -4,
};

struct DenseSeparorInfo {
    unsigned fenceLen;
    unsigned lowerCount;
};


struct DenseNode {
    Tag tag;
    uint16_t fullKeyLen;
    NumericPart arrayStart;
    union {
        uint16_t spaceUsed;
        uint16_t valLen;
    };
    uint16_t slotCount;
    uint16_t occupiedCount;
    uint16_t lowerFenceLen;
    union {
        struct {
            uint16_t upperFenceLen;
            uint16_t prefixLength;
            uint16_t _mask_pad[2];
            Mask mask[(pageSizeLeaf - 24) / sizeof(Mask)];
        };
        struct {
            uint16_t _union_pad[2];
            uint16_t dataOffset;
            uint16_t slots[(pageSizeLeaf - 22) / sizeof(uint16_t)];
        };
        uint8_t _expand_heap[pageSizeLeaf - 16];
    };

    unsigned fencesOffset();

    uint8_t *getLowerFence();

    uint8_t *getUpperFence();

    AnyNode *any();

    uint8_t *getPrefix();

    static void restoreKey(NumericPart arrayStart, unsigned fullKeyLen, uint8_t *prefix, uint8_t *dst, unsigned index);

    void changeUpperFence(uint8_t *fence, unsigned len);

    void copyKeyValueRangeToBasic(BTreeNode *dst, unsigned srcStart, unsigned srcEnd);

    bool insert(uint8_t *key, unsigned keyLength, uint8_t *payload, unsigned payloadLength);

    void splitNode1(AnyNode *parent, uint8_t *key, unsigned keyLen);

    void splitNode2(AnyNode *parent, uint8_t *key, unsigned keyLen);

    unsigned prefixDiffLen();

    KeyError keyToIndex(uint8_t *truncatedKey, unsigned truncatedLen);

    static unsigned computeNumericPartLen(unsigned fullKeyLen);

    static unsigned computeNumericPrefixLength(unsigned fullKeyLen);

    void changeLowerFence(uint8_t *lowerFence, unsigned lowerFenceLen, uint8_t *upperFence, unsigned upperFenceLen);

    static bool densify1(DenseNode *out, BTreeNode *basicNode);

    static bool densify2(DenseNode *out, BTreeNode *from);

    static DenseSeparorInfo densifySplit(uint8_t *sepBuffer, BTreeNode *basicNode);

    void init2b(uint8_t *lowerFence, unsigned lowerFenceLen, uint8_t *upperFence, unsigned upperFenceLen,
                unsigned fullKeyLen, unsigned slotCount);

    int cmpNumericPrefix(uint8_t *key, unsigned length);

    unsigned maskWordCount();

    void zeroMask();

    void zeroSlots();

    // rounds down
    static NumericPart getNumericPart(uint8_t *key, unsigned length, unsigned targetLength);

    static NumericPart leastGreaterKey(uint8_t *key, unsigned length, unsigned targetLength);

    void updateArrayStart();

    uint8_t *ptr();

    static unsigned computeSlotCount(unsigned valLen, unsigned fencesStart);

    bool try_densify(BTreeNode *basicNode);

    bool isSlotPresent(unsigned i);

    void setSlotPresent(unsigned i);

    void insertSlotWithSpace(unsigned i, uint8_t *payload, unsigned payloadLen);

    bool requestSpaceFor(unsigned payloadLen);

    unsigned slotEndOffset();

    unsigned slotValLen(unsigned index);

    void unsetSlotPresent(unsigned i);

    uint8_t *getVal(unsigned i);

    uint8_t *lookup(uint8_t *key, unsigned int keyLength, unsigned int &payloadSizeOut);

    void updatePrefixLength();

    bool remove(uint8_t *key, unsigned int keyLength);

    bool is_underfull();

    BTreeNode *convertToBasic();

    bool range_lookup1(uint8_t *key,
                       unsigned int keyLen,
                       uint8_t *keyOut,
                       const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    bool range_lookup2(uint8_t *key,
                       unsigned int keyLen,
                       uint8_t *keyOut,
                       const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    bool range_lookup_desc(uint8_t *key,
                           unsigned int keyLen,
                           uint8_t *keyOut,
                           const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb);

    bool isNumericRangeAnyLen(uint8_t *key, unsigned length);

    void print();
};


#endif //BTREE24_DENSENODE_HPP
