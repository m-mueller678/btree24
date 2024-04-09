#include "HashNode.hpp"
#include "AnyNode.hpp"
#include "common.hpp"

GuardX<AnyNode> HashNode::makeRootLeaf() {
    auto node = AnyNode::allocLeaf();

    node->_hash.init({}, {}, pageSizeLeaf / 64, RangeOpCounter{});
    return node;
}

void HashNode::init(std::span<uint8_t> lowerFence, std::span<uint8_t> upperFence, unsigned int hashCapacity,
                    RangeOpCounter roc) {
    assert(sizeof(HashNode) == pageSizeLeaf);
    set_tag(Tag::Hash);
    rangeOpCounter = roc;
    count = 0;
    sortedCount = 0;
    spaceUsed = upperFenceLen + lowerFenceLen;
    dataOffset = pageSizeLeaf - spaceUsed - hashCapacity;
    hashOffset = dataOffset;
    this->hashCapacity = hashCapacity;
    this->lowerFenceLen = lowerFenceLen;
    this->upperFenceLen = upperFenceLen;
    copySpan(getLowerFence(), lowerFence);
    copySpan(getUpperFence(), upperFence);
    updatePrefixLength();
}

void HashNode::updatePrefixLength() {
    auto uf = getUpperFence();
    auto lf = getLowerFence();
    prefixLength = 0;
    while (prefixLength < lf.size() && prefixLength < uf.size() && lf[prefixLength] == uf[prefixLength]) {
        prefixLength += 1;
    }
}

std::span<uint8_t> HashNode::getLowerFence() {
    return slice(pageSizeLeaf - lowerFenceLen, lowerFenceLen);
}

std::span<uint8_t> HashNode::getUpperFence() {
    return slice(pageSizeLeaf - lowerFenceLen - upperFenceLen, upperFenceLen);
}

std::span<uint8_t> HashNode::slice(uint16_t offset, uint16_t len) {
    if (uint32_t(offset) + uint32_t(len) > pageSize)
        throw OLCRestartException{};
    return {ptr() + offset, ptr() + offset + len};
}


std::span<uint8_t> HashNode::getKey(unsigned slotId) {
    return slice(slot[slotId].offset, slot[slotId].keyLen);
}

std::span<uint8_t> HashNode::getPayload(unsigned slotId) {
    return slice(slot[slotId].offset + slot[slotId].keyLen, slot[slotId].payloadLen);
}

uint8_t *HashNode::ptr() {
    return reinterpret_cast<uint8_t *>(this);
}