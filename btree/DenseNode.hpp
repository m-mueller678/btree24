#ifndef BTREE24_DENSENODE_HPP
#define BTREE24_DENSENODE_HPP


#include <cstdint>
#include <functional>
#include <c++/11/span>
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


struct DenseNode : TagAndDirty {
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

    std::span<uint8_t> slice(uint16_t offset, uint16_t len);

    std::span<uint8_t> getLowerFence();

    std::span<uint8_t> getUpperFence();

    AnyNode *any();

    std::span<uint8_t> getPrefix();

    static void restoreKey(NumericPart arrayStart, unsigned fullKeyLen, uint8_t *prefix, uint8_t *dst, unsigned index);

    void changeUpperFence(std::span<uint8_t> fence);

    void copyKeyValueRangeToBasic(BTreeNode *dst, unsigned srcStart, unsigned srcEnd);

    bool insert(std::span<uint8_t> key, std::span<uint8_t> payload);

    void splitNode1(AnyNode *parent, std::span<uint8_t> key);

    void splitNode2(AnyNode *parent, std::span<uint8_t> key);

    unsigned prefixDiffLen();

    KeyError keyToIndex(std::span<uint8_t> truncatedKey);

    static unsigned computeNumericPartLen(unsigned fullKeyLen);

    static unsigned computeNumericPrefixLength(unsigned fullKeyLen);

    void changeLowerFence(std::span<uint8_t> lowerFence, std::span<uint8_t> upperFence);

    static bool densify1(DenseNode *out, BTreeNode *basicNode);

    static bool densify2(DenseNode *out, BTreeNode *from);

    static DenseSeparorInfo densifySplit(uint8_t *sepBuffer, BTreeNode *basicNode);

    void init2b(std::span<uint8_t> lowerFence, std::span<uint8_t> upperFence,
                unsigned fullKeyLen, unsigned slotCount);

    int cmpNumericPrefix(std::span<uint8_t> key);

    unsigned maskWordCount();

    void zeroMask();

    void zeroSlots();

    // rounds down
    static NumericPart getNumericPart(std::span<uint8_t> key, unsigned targetLength);

    static NumericPart leastGreaterKey(std::span<uint8_t> key, unsigned targetLength);

    void updateArrayStart();

    uint8_t *ptr();

    static unsigned computeSlotCount(unsigned valLen, unsigned fencesStart);

    bool try_densify(BTreeNode *basicNode);

    bool isSlotPresent(unsigned i);

    void setSlotPresent(unsigned i);

    void insertSlotWithSpace(unsigned i, std::span<uint8_t> payload);

    bool requestSpaceFor(unsigned payloadLen);

    unsigned slotEndOffset();

    unsigned slotValLen(unsigned index);

    void unsetSlotPresent(unsigned i);

    std::span<uint8_t> getValD1(unsigned i);

    std::span<uint8_t> getValD2(unsigned i);

    void lookup(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void updatePrefixLength();

    bool remove(std::span<uint8_t> key);

    bool is_underfull();

    BTreeNode *convertToBasic();

    bool range_lookup1(std::span<uint8_t> key,
                       uint8_t *keyOut,
                       const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);

    bool range_lookup2(std::span<uint8_t> key,
                       uint8_t *keyOut,
                       const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);


    bool isNumericRangeAnyLen(std::span<uint8_t> key);

    void validate();

    void print();
};


#endif //BTREE24_DENSENODE_HPP
