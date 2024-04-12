#include "DenseNode.hpp"
#include "common.hpp"
#include "vmache.hpp"
#include "BTreeNode.hpp"
#include "AnyNode.hpp"

unsigned DenseNode::fencesOffset() {
    return pageSizeLeaf - lowerFenceLen - max(upperFenceLen, fullKeyLen);
}


std::span<uint8_t> DenseNode::getLowerFence() {
    return slice(pageSizeLeaf - lowerFenceLen, lowerFenceLen);
}

std::span<uint8_t> DenseNode::getUpperFence() {
    return slice(fencesOffset(), upperFenceLen);
}

std::span<uint8_t> DenseNode::slice(uint16_t offset, uint16_t len) {
    if (uint32_t(offset) + uint32_t(len) > pageSize)
        throw OLCRestartException{};
    return {ptr() + offset, ptr() + offset + len};
}

std::span<uint8_t> DenseNode::getValD1(unsigned slotId) {
    return slice(+offsetof(DenseNode, mask) + maskWordCount() * maskBytesPerWord + slotId * valLen, valLen);
}

std::span<uint8_t> DenseNode::getValD2(unsigned slotId) {
    return slice(slots[slotId] + 2, slotValLen(slotId));
}

bool DenseNode::lookup(std::span<uint8_t> key, std::span<uint8_t> &payloadOut) {
    KeyError index = keyToIndex(key);
    if (index < 0)
        return false;
    if (tag() == Tag::Dense) {
        if (!isSlotPresent(index)) {
            return false;
        }
        if (valLen > maxKvSize)
            throw OLCRestartException();
        payloadOut = {payloadOut.data(), valLen};
        copySpan(payloadOut, getValD1(index));
        return true;
    } else {
        if (!slots[index])
            return false;
        payloadOut = {payloadOut.data(), slotValLen(index)};
        copySpan(payloadOut, getValD2(index));
        return true;
    }
}

AnyNode *DenseNode::any() {
    return reinterpret_cast<AnyNode *>(this);
}

std::span<uint8_t> DenseNode::getPrefix() {
    return getLowerFence().subspan(0, prefixLength);
}

void DenseNode::restoreKey(NumericPart arrayStart, unsigned fullKeyLen, uint8_t *prefix, uint8_t *dst, unsigned index) {
    unsigned numericPartLen = computeNumericPartLen(fullKeyLen);
    memcpy(dst, prefix, fullKeyLen - numericPartLen);
    NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(index));
    memcpy(dst + fullKeyLen - numericPartLen,
           reinterpret_cast<uint8_t *>(&numericPart) + sizeof(NumericPart) - numericPartLen, numericPartLen);
}

void DenseNode::changeUpperFence(std::span<uint8_t> fence) {
    ASSUME(fence.size() <= fullKeyLen);
    upperFenceLen = fence.size();
    copySpan(getUpperFence(), fence);
    updatePrefixLength();
}

void DenseNode::updatePrefixLength() {
    prefixLength = commonPrefixLength(getLowerFence(), getUpperFence());
}

void DenseNode::copyKeyValueRangeToBasic(BTreeNode *dst, unsigned srcStart, unsigned srcEnd) {
    assert(dst->prefixLength >= prefixLength);
    assert(dst->count == 0);
    unsigned npLen = computeNumericPartLen(fullKeyLen);
    unsigned outSlot = 0;
    for (unsigned i = srcStart; i < srcEnd; i++) {
        if (!isSlotPresent(i)) {
            continue;
        }
        NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(i));
        unsigned newKeyLength = fullKeyLen - dst->prefixLength;
        unsigned space = newKeyLength + valLen;
        dst->dataOffset -= space;
        dst->spaceUsed += space;
        dst->slot[outSlot].offset = dst->dataOffset;
        dst->slot[outSlot].keyLen = fullKeyLen - dst->prefixLength;
        dst->slot[outSlot].payloadLen = valLen;
        if (fullKeyLen - npLen > dst->prefixLength) {
            memcpy(dst->getKey(outSlot).data(), getPrefix().data() + dst->prefixLength,
                   fullKeyLen - npLen - dst->prefixLength);
            memcpy(dst->getKey(outSlot).data() + fullKeyLen - npLen - dst->prefixLength,
                   reinterpret_cast<uint8_t *>(&numericPart) + sizeof(NumericPart) - npLen,
                   npLen);
        } else {
            unsigned truncatedNumericPartLen = fullKeyLen - dst->prefixLength;
            memcpy(dst->getKey(outSlot).data(),
                   reinterpret_cast<uint8_t *>(&numericPart) + sizeof(NumericPart) - truncatedNumericPartLen,
                   truncatedNumericPartLen);
        }
        if (tag() == Tag::Dense) {
            copySpan(dst->getPayload(outSlot), getValD1(i));
        } else {
            copySpan(dst->getPayload(outSlot), getValD2(i));
        }
        if (enableBasicHead)
            dst->slot[outSlot].head[0] = head(dst->getKey(outSlot));
        outSlot += 1;
    }
    dst->count = outSlot;
    assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t *>(dst->slot + dst->count));
}

bool DenseNode::insert(std::span<uint8_t> key, std::span<uint8_t> payload) {
    ASSUME(key.size() >= prefixLength);
    if (tag() == Tag::Dense) {
        if (payload.size() != valLen || key.size() != fullKeyLen) {
            unsigned entrySize = occupiedCount * (fullKeyLen - prefixLength + payload.size() + sizeof(BTreeNode::Slot));
            if (entrySize + lowerFenceLen + upperFenceLen + sizeof(BTreeNodeHeader) <= pageSizeLeaf) {
                BTreeNode *basicNode = convertToBasic();
                return basicNode->insert(key, payload);
            } else {
                return false;
            }
        }
        KeyError keyIndex = keyToIndex(key);
        switch (keyIndex) {
            case KeyError::SlightlyTooLarge:
            case KeyError::FarTooLarge:
            case KeyError::NotNumericRange:
                return false;
            case KeyError::WrongLen:
                ASSUME(false);
        }
        assert(keyIndex >= 0);
        occupiedCount += !isSlotPresent(keyIndex);
        setSlotPresent(keyIndex);
        copySpan(getValD1(keyIndex), payload);
        return true;
    } else {
        if (key.size() != fullKeyLen) {
            unsigned entrySize = occupiedCount * (fullKeyLen - prefixLength + sizeof(BTreeNode::Slot) - 2) + spaceUsed;
            if (entrySize + lowerFenceLen + upperFenceLen + sizeof(BTreeNodeHeader) <= pageSizeLeaf) {
                BTreeNode *basicNode = convertToBasic();
                return basicNode->insert(key, payload);
            } else {
                return false;
            }
        }
        KeyError keyIndex = keyToIndex(key);
        switch (keyIndex) {
            case KeyError::SlightlyTooLarge:
            case KeyError::FarTooLarge:
            case KeyError::NotNumericRange:
                return false;
            case KeyError::WrongLen:
                ASSUME(false);
        }
        assert(keyIndex >= 0);
        if (slots[keyIndex]) {
            spaceUsed -= loadUnaligned<uint16_t>(ptr() + slots[keyIndex]) + 2;
            slots[keyIndex] = 0;
            occupiedCount -= 1;
        }
        if (!requestSpaceFor(payload.size()))
            return false;
        insertSlotWithSpace(keyIndex, payload);
        occupiedCount += 1;
        return true;
    }
}

BTreeNode *DenseNode::convertToBasic() {
    TmpBTreeNode tmp;
    tmp.node.init(true, RangeOpCounter{});
    tmp.node.setFences(getLowerFence(), getUpperFence());
    copyKeyValueRangeToBasic(&tmp.node, 0, slotCount);
    tmp.node.makeHint();
    BTreeNode *basicNode = reinterpret_cast<BTreeNode *>(this);
    memcpy(basicNode, &tmp.node, pageSizeLeaf);
    return basicNode;
}

void DenseNode::splitNode1(AnyNode *parent, std::span<uint8_t> key) {
    assert(key.size() >= prefixLength);
    int key_index = keyToIndex(key);
    bool split_to_self;
    switch (key_index) {
        case KeyError::FarTooLarge:
        case KeyError::NotNumericRange:
            split_to_self = false;
            break;
        case KeyError::WrongLen:
            // splitting into two new basic nodes might be impossible if prefix length is short
            if (upperFenceLen < fullKeyLen) {
                split_to_self = false;
            } else {
                // TODO split to two basic nodes instead
                split_to_self = false;
            }
            break;
        case KeyError::SlightlyTooLarge:
            split_to_self = true;
            break;
    }
    uint8_t full_boundary[fullKeyLen];
    restoreKey(arrayStart, fullKeyLen, getLowerFence().data(), full_boundary, slotCount - 1);

    auto left = AnyNode::allocLeaf();
    memcpy(left.ptr, this, sizeof(DenseNode));
    bool succ = parent->insertChild({full_boundary, fullKeyLen}, left.pid);
    assert(succ);
    auto denseLeft = left->dense();
    if (split_to_self) {
        this->changeLowerFence({full_boundary, denseLeft->fullKeyLen}, denseLeft->getUpperFence());
    } else {
        BTreeNode *right = &this->any()->_basic_node;
        right->init(true, RangeOpCounter{});
        right->setFences({full_boundary, denseLeft->fullKeyLen}, denseLeft->getUpperFence());
    }
    denseLeft->changeUpperFence({full_boundary, denseLeft->fullKeyLen});
}

void DenseNode::splitNode2(AnyNode *parent, std::span<uint8_t> key) {
    assert(key.size() >= prefixLength);
    auto left = AnyNode::allocLeaf();
    auto leftDense = &left->_dense;
    uint8_t splitKeyBuffer[fullKeyLen];
    unsigned totalSpace = occupiedCount * 2 + spaceUsed;
    unsigned leftSpace = 0;
    unsigned splitSlot = 0;
    while (leftSpace < totalSpace / 2) {
        ASSUME(splitSlot < slotCount);
        if (slots[splitSlot]) {
            leftSpace += 2 + 2 + slotValLen(splitSlot);
        }
        splitSlot += 1;
    }
    restoreKey(arrayStart, fullKeyLen, getLowerFence().data(), splitKeyBuffer, splitSlot);
    leftDense->init2b(getLowerFence(), {splitKeyBuffer, fullKeyLen}, fullKeyLen, splitSlot + 1);
    for (unsigned i = 0; i <= splitSlot; ++i) {
        if (slots[i])
            leftDense->insertSlotWithSpace(i, getValD2(i));
    }
    DenseNode right;
    right.init2b({splitKeyBuffer, fullKeyLen}, getUpperFence(), fullKeyLen, slotCount - splitSlot - 1);
    for (unsigned i = splitSlot + 1; i < slotCount; ++i) {
        if (slots[i])
            right.insertSlotWithSpace(i - splitSlot - 1, getValD2(i));
    }
    memcpy(this, &right, pageSizeLeaf);
    bool succ = parent->insertChild({splitKeyBuffer, leftDense->fullKeyLen}, left.pid);
    ASSUME(succ);
}

KeyError DenseNode::keyToIndex(std::span<uint8_t> key) {
    if (key.size() != fullKeyLen)
        return KeyError::WrongLen;
    if (prefixLength + sizeof(NumericPart) < fullKeyLen &&
        memcmp(getLowerFence().data() + prefixLength, key.data() + prefixLength,
               fullKeyLen - sizeof(NumericPart) - prefixLength) != 0)
        return KeyError::NotNumericRange;
    NumericPart numericPart = getNumericPart(key, fullKeyLen);
    assert(numericPart >= arrayStart);
    NumericPart index = numericPart - arrayStart;
    if (index < slotCount) {
        return static_cast<KeyError>(index);
    } else if (index < slotCount + slotCount / 2) {
        // TODO might scrap this distinction
        return KeyError::SlightlyTooLarge;
    } else {
        return KeyError::FarTooLarge;
    }
}

unsigned DenseNode::computeNumericPartLen(unsigned fullKeyLen) {
    return min(maxNumericPartLen, fullKeyLen);
}

unsigned DenseNode::computeNumericPrefixLength(unsigned fullKeyLen) {
    return fullKeyLen - computeNumericPartLen(fullKeyLen);
}

void DenseNode::changeLowerFence(std::span<uint8_t> lowerFence, std::span<uint8_t> upperFence) {
    assert(enablePrefix);
    assert(sizeof(DenseNode) == pageSizeLeaf);
    set_tag(Tag::Dense);
    this->lowerFenceLen = lowerFence.size();
    occupiedCount = 0;
    slotCount = computeSlotCount(valLen, fencesOffset());
    zeroMask();
    copySpan(getLowerFence(), lowerFence);
    copySpan(getUpperFence(), upperFence);
    this->updatePrefixLength();
    assert(computeNumericPrefixLength(fullKeyLen) <= lowerFenceLen);
    updateArrayStart();
}

bool DenseNode::densify1(DenseNode *out, BTreeNode *basicNode) {
    unsigned preKeyLen1 = basicNode->slot[0].keyLen;
    unsigned fullKeyLen = preKeyLen1 + basicNode->prefixLength;
    // COUNTER(reject_0, basicNode->lowerFence.length + sizeof(NumericPart) < fullKeyLen, 1 << 8);
    if (basicNode->lowerFence.length + sizeof(NumericPart) < fullKeyLen) {
        // this might be possible to handle, but requires more thought and should be rare.
        return false;
    }
    unsigned valLen1 = basicNode->slot[0].payloadLen;
    for (unsigned i = 1; i < basicNode->count; ++i) {
        if (basicNode->slot[i].keyLen != preKeyLen1 || basicNode->slot[i].payloadLen != valLen1) {
            return false;
        }
    }
    out->lowerFenceLen = basicNode->lowerFence.length;
    out->upperFenceLen = basicNode->upperFence.length;
    out->fullKeyLen = fullKeyLen;
    out->arrayStart = leastGreaterKey(basicNode->getLowerFence(), fullKeyLen);
    out->slotCount = computeSlotCount(valLen1, out->fencesOffset());
    NumericPart prefixNumericPart = getNumericPart(basicNode->getPrefix(), fullKeyLen);
    {
        auto lastKey = basicNode->getKey(basicNode->count - 1);
        bool lastOutsideRange = false;
        for (unsigned i = basicNode->prefixLength; i + sizeof(NumericPart) < fullKeyLen; ++i) {
            if (lastKey[i - basicNode->prefixLength] != basicNode->getLowerFence()[i]) {
                lastOutsideRange = true;
                break;
            }
        }
        NumericPart lastKeyNumericPart = getNumericPart(basicNode->getKey(basicNode->count - 1), preKeyLen1);
        lastOutsideRange |= prefixNumericPart + lastKeyNumericPart - out->arrayStart >= out->slotCount;
        // COUNTER(reject_last, lastOutsideRange, 1 << 8);
        if (lastOutsideRange)
            return false;
    }
    out->set_tag(Tag::Dense);
    out->valLen = valLen1;
    out->occupiedCount = 0;
    out->zeroMask();
    copySpan(out->getLowerFence(), basicNode->getLowerFence());
    copySpan(out->getUpperFence(), basicNode->getUpperFence());
    out->prefixLength = basicNode->prefixLength;
    assert(computeNumericPrefixLength(fullKeyLen) <= out->lowerFenceLen);
    for (unsigned i = 0; i < basicNode->count; ++i) {
        NumericPart keyNumericPart = getNumericPart(basicNode->getKey(i), fullKeyLen - out->prefixLength);
        int index = prefixNumericPart + keyNumericPart - out->arrayStart;
        out->setSlotPresent(index);
        copySpan(out->getValD1(index), basicNode->getPayload(i));
    }
    out->occupiedCount = basicNode->count;
    return true;
}

DenseSeparorInfo DenseNode::densifySplit(uint8_t *sepBuffer, BTreeNode *basicNode) {
    unsigned minTake = basicNode->count / 2;
    unsigned preKeyLen1 = basicNode->slot[0].keyLen;
    unsigned fullKeyLen = preKeyLen1 + basicNode->prefixLength;
    // COUNTER(reject_0, basicNode->lowerFence.length + sizeof(NumericPart) < fullKeyLen, 1);
    if (basicNode->lowerFence.length + sizeof(NumericPart) < fullKeyLen) {
        // this might be possible to handle, but requires more thought and should be rare.
        return DenseSeparorInfo{0, 0};
    }
    unsigned valLen1 = basicNode->slot[0].payloadLen;
    unsigned equalLen = 1;
    for (; equalLen < basicNode->count && basicNode->slot[equalLen].keyLen == preKeyLen1 &&
           basicNode->slot[equalLen].payloadLen == valLen1;
           ++equalLen);
    if (equalLen < minTake) {
        return DenseSeparorInfo{0, 0};
    }
    NumericPart arrayStart = leastGreaterKey(basicNode->getLowerFence(), fullKeyLen);
    unsigned slotCount = computeSlotCount(valLen1, pageSizeLeaf - 2 * basicNode->lowerFence.length);
    NumericPart prefixNumericPart = getNumericPart(basicNode->getPrefix(), fullKeyLen);
    restoreKey(arrayStart, fullKeyLen, basicNode->getLowerFence().data(), sepBuffer, slotCount - 1);
    unsigned takeKeyCount;
    {
        bool found;
        unsigned lowerBound = basicNode->lowerBound({sepBuffer, basicNode->lowerFence.length}, found);
        takeKeyCount = lowerBound + found;
    }
    if (takeKeyCount > equalLen)
        takeKeyCount = equalLen;
    if (takeKeyCount < minTake)
        return DenseSeparorInfo{0, 0};
    unsigned fenceLen = basicNode->slot[takeKeyCount].keyLen + basicNode->prefixLength;
    memcpy(sepBuffer + basicNode->prefixLength, basicNode->getKey(takeKeyCount).data(),
           fenceLen - basicNode->prefixLength);
    // generate separator between next key and max key
    {
        if (fenceLen == fullKeyLen) {
            if (sepBuffer[fenceLen - 1] > 0)
                sepBuffer[fenceLen - 1] -= 1;
            else {
                fenceLen -= 1;
            }
        } else if (fenceLen < fullKeyLen) {
            sepBuffer[fenceLen - 1] -= 1;
            for (; fenceLen < fullKeyLen; ++fenceLen) {
                sepBuffer[fenceLen] = 255;
            }
        } else if (fenceLen > fullKeyLen) {
            fenceLen -= 1;
        }
    }
    return DenseSeparorInfo{fenceLen, takeKeyCount};
}

bool DenseNode::densify2(DenseNode *out, BTreeNode *from) {
    assert(enablePrefix);
    assert(sizeof(DenseNode) == pageSizeLeaf);
    unsigned keyLen = from->slot[0].keyLen + from->prefixLength;
    // COUNTER(reject_lower_short, from->lowerFence.length + sizeof(NumericPart) < keyLen, 1 << 8);
    if (from->lowerFence.length + sizeof(NumericPart) < keyLen)
        return false;
    // COUNTER(reject_upper, from->upperFence.length == 0, 1 << 8);
    if (from->upperFence.length == 0)
        return false;
    {
        bool upperOutsideRange = false;
        for (unsigned i = from->prefixLength; i < from->upperFence.length && i + sizeof(NumericPart) < keyLen; ++i) {
            if (from->getUpperFence()[i] != from->getLowerFence()[i]) {
                upperOutsideRange = true;
                break;
            }
        }
        // COUNTER(reject_upper2, upperOutsideRange, 1 << 8);
        if (upperOutsideRange)
            return false;
    }
    NumericPart arrayStart = leastGreaterKey(from->getLowerFence(), keyLen);
    NumericPart arrayEnd = leastGreaterKey(from->getUpperFence(), keyLen);
    ASSUME(arrayStart < arrayEnd);
    if (arrayEnd - arrayStart >= pageSizeLeaf / 2)
        return false;
    for (unsigned i = 1; i < from->count; ++i) {
        if (from->slot[i].keyLen != from->slot[0].keyLen) {
            return false;
        }
    }
    unsigned totalPayloadSize = 0;
    out->slotCount = arrayEnd - arrayStart;
    out->lowerFenceLen = from->lowerFence.length;
    out->upperFenceLen = from->upperFence.length;
    out->fullKeyLen = keyLen;
    out->dataOffset = out->fencesOffset();
    {
        bool payloadSizeTooLarge = false;
        for (unsigned i = 0; i < from->count; ++i) {
            totalPayloadSize += from->slot[i].payloadLen;
            if (out->slotEndOffset() + totalPayloadSize + 2 * from->count > out->dataOffset) {
                payloadSizeTooLarge = true;
                break;
            }
        }
        // COUNTER(reject_payload_size, payloadSizeTooLarge, 1 << 8);
        if (payloadSizeTooLarge)
            return false;
    }
    out->set_tag(Tag::Dense2);
    out->spaceUsed = 0;
    out->occupiedCount = 0;
    copySpan(out->getLowerFence(), from->getLowerFence());
    copySpan(out->getUpperFence(), from->getUpperFence());
    out->prefixLength = from->prefixLength;
    out->arrayStart = arrayStart;
    out->zeroSlots();
    NumericPart prefixNumericPart = getNumericPart(out->getPrefix(), out->fullKeyLen);
    for (unsigned i = 0; i < from->count; ++i) {
        NumericPart keyNumericPart = getNumericPart(from->getKey(i), out->fullKeyLen - out->prefixLength);
        int index = prefixNumericPart + keyNumericPart - arrayStart;
        out->insertSlotWithSpace(index, from->getPayload(i));
    }
    out->occupiedCount = from->count;
    return true;
}

void DenseNode::init2b(std::span<uint8_t> lowerFence,
                       std::span<uint8_t> upperFence,
                       unsigned fullKeyLen,
                       unsigned slotCount) {
    assert(enablePrefix);
    assert(sizeof(DenseNode) == pageSizeLeaf);
    set_tag(Tag::Dense2);
    this->fullKeyLen = fullKeyLen;
    this->spaceUsed = 0;
    this->lowerFenceLen = lowerFence.size();
    this->upperFenceLen = upperFence.size();
    occupiedCount = 0;
    this->dataOffset = fencesOffset();
    copySpan(this->getLowerFence(), lowerFence);
    copySpan(this->getUpperFence(), upperFence);
    this->updatePrefixLength();
    this->updateArrayStart();
    this->slotCount = slotCount;
    zeroSlots();
    assert(computeNumericPrefixLength(fullKeyLen) <= lowerFenceLen);
}

bool DenseNode::is_underfull() {
    unsigned totalEntrySize = (fullKeyLen - prefixLength + valLen + sizeof(BTreeNode::Slot)) * occupiedCount;
    return sizeof(BTreeNodeHeader) + totalEntrySize + lowerFenceLen + upperFenceLen <
           pageSizeLeaf - BTreeNode::underFullSizeLeaf;
}

unsigned DenseNode::maskWordCount() {
    return (slotCount + maskBitsPerWord - 1) / maskBitsPerWord;
}

void DenseNode::zeroMask() {
    ASSUME(tag() == Tag::Dense);
    unsigned mwc = maskWordCount();
    for (unsigned i = 0; i < mwc; ++i) {
        mask[i] = 0;
    }
}

void DenseNode::zeroSlots() {
    ASSUME(tag() == Tag::Dense2);
    for (unsigned i = 0; i < slotCount; ++i) {
        slots[i] = 0;
    }
}

// key is expected to be not prefix truncated
// TODO inspect assembly
NumericPart DenseNode::getNumericPart(std::span<uint8_t> key, unsigned targetLength) {
    if (key.size() > targetLength)
        key = key.subspan(0, targetLength);
    if (key.size() + sizeof(NumericPart) <= targetLength) {
        return 0;
    }
    NumericPart x;
    switch (key.size()) {
        case 0:
            x = 0;
            break;
        case 1:
            x = static_cast<uint32_t>(key[0]);
            break;
        case 2:
            x = static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key.data())));
            break;
        case 3:
            x = (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key.data()))) << 8) |
                (static_cast<uint32_t>(key[2]));
            break;
        default:
            x = __builtin_bswap32(loadUnaligned<uint32_t>(key.data() + key.size() - 4));
            break;
    }
    return x << (8 * (targetLength - key.size()));
}

NumericPart DenseNode::leastGreaterKey(std::span<uint8_t> key, unsigned targetLength) {
    auto a = getNumericPart(key, targetLength);
    assert(a < UINT32_MAX);  // TODO we should check this, but none of our key sets run into this issue
    auto b = key.size() >= targetLength;
    return a + b;
}

void DenseNode::updateArrayStart() {
    arrayStart = leastGreaterKey(getLowerFence(), fullKeyLen);
}

uint8_t *DenseNode::ptr() {
    return reinterpret_cast<uint8_t *>(this);
}

unsigned DenseNode::computeSlotCount(unsigned valLen, unsigned fencesStart) {
    unsigned count = fencesStart * 8 / (valLen * 8 + 1);
    while (true) {
        unsigned maskSize = (count + maskBitsPerWord - 1) / maskBitsPerWord * maskBytesPerWord;
        if (offsetof(DenseNode, mask) + maskSize + count * valLen > fencesStart) {
            count -= 1;
        } else {
            return count;
        }
    }
}

bool DenseNode::remove(std::span<uint8_t> key) {
    assert(tag() == Tag::Dense);  // dense2 remove is not implemented
    KeyError index = keyToIndex(key);
    if (index < 0 || !isSlotPresent(index)) {
        return false;
    }
    unsetSlotPresent(index);
    occupiedCount -= 1;
    return true;
}

bool DenseNode::try_densify(BTreeNode *basicNode) {
    assert(!enableDense || !enableDense2);
    assert(basicNode->count > 0);
    bool success = enableDense ? densify1(this, basicNode) : densify2(this, basicNode);
    // COUNTER(reject, !success, 1 << 8);
    return success;
}

bool DenseNode::isSlotPresent(unsigned i) {
    assert(i < slotCount);
    return (mask[i / maskBitsPerWord] >> (i % maskBitsPerWord) & 1) != 0;
}

void DenseNode::setSlotPresent(unsigned i) {
    assert(i < slotCount);
    mask[i / maskBitsPerWord] |= Mask(1) << (i % maskBitsPerWord);
}

void DenseNode::unsetSlotPresent(unsigned i) {
    assert(i < slotCount);
    mask[i / maskBitsPerWord] &= ~(Mask(1) << (i % maskBitsPerWord));
}
//
//bool DenseNode::range_lookup1(uint8_t* key,
//                              unsigned keyLen,
//                              uint8_t* keyOut,
//        // called with keylen and value
//        // scan continues if callback returns true
//                              const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
//{
//    if (!isNumericRangeAnyLen(key))
//        return true;
//    unsigned firstIndex = (key == nullptr) ? 0 : (leastGreaterKey(key, keyLen, fullKeyLen) - (keyLen == fullKeyLen) - arrayStart);
//    unsigned nprefLen = computeNumericPrefixLength(fullKeyLen);
//    if (nprefLen > prefixLength) {
//        memcpy(keyOut + prefixLength, getLowerFence() + prefixLength, nprefLen - prefixLength);
//    }
//
//    unsigned wordIndex = firstIndex / maskBitsPerWord;
//    Mask word = mask[wordIndex];
//    unsigned shift = firstIndex % maskBitsPerWord;
//    word >>= shift;
//    while (true) {
//        unsigned trailingZeros = std::__countr_zero(word);
//        if (trailingZeros == maskBitsPerWord) {
//            wordIndex += 1;
//            if (wordIndex >= maskWordCount()) {
//                return true;
//            }
//            shift = 0;
//            word = mask[wordIndex];
//        } else {
//            shift += trailingZeros;
//            word >>= trailingZeros;
//            unsigned entryIndex = wordIndex * maskBitsPerWord + shift;
//            if (entryIndex > slotCount) {
//                return true;
//            }
//            NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(entryIndex));
//            unsigned numericPartLen = fullKeyLen - nprefLen;
//            memcpy(keyOut + nprefLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - numericPartLen, numericPartLen);
//            if (!found_record_cb(fullKeyLen, getValD1(entryIndex), valLen)) {
//                return false;
//            }
//            shift += 1;
//            word >>= 1;
//        }
//    }
//}
//
//bool DenseNode::range_lookup2(uint8_t* key,
//                              unsigned keyLen,
//                              uint8_t* keyOut,
//        // called with keylen and value
//        // scan continues if callback returns true
//                              const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
//{
//    unsigned firstIndex = (key == nullptr) ? 0 : (leastGreaterKey(key, keyLen, fullKeyLen) - (keyLen == fullKeyLen) - arrayStart);
//    unsigned nprefLen = computeNumericPrefixLength(fullKeyLen);
//    if (nprefLen > prefixLength) {
//        memcpy(keyOut + prefixLength, getLowerFence() + prefixLength, nprefLen - prefixLength);
//    }
//    for (unsigned i = firstIndex; i < slotCount; ++i) {
//        if (slots[i]) {
//            NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(i));
//            unsigned numericPartLen = fullKeyLen - nprefLen;
//            memcpy(keyOut + nprefLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - numericPartLen, numericPartLen);
//            if (!found_record_cb(fullKeyLen, ptr() + slots[i] + 2, slotValLen(i))) {
//                return false;
//            }
//        }
//    }
//    return true;
//}
//
//bool DenseNode::range_lookup_desc(uint8_t* key,
//                                  unsigned keyLen,
//                                  uint8_t* keyOut,
//        // called with keylen and value
//        // scan continues if callback returns true
//                                  const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
//{
//    abort();  // this function is currently broken
//    int firstIndex = isNumericRangeAnyLen(key, keyLen) ? (int(leastGreaterKey(key, keyLen, fullKeyLen)) - 1 - (keyLen != fullKeyLen) - arrayStart)
//                                                       : (slotCount - 1);
//    if (firstIndex < 0)
//        return true;
//    unsigned nprefLen = computeNumericPrefixLength(fullKeyLen);
//    memcpy(keyOut, getLowerFence(), nprefLen);
//
//    int wordIndex = firstIndex / maskBitsPerWord;
//    Mask word = mask[wordIndex];
//    unsigned shift = (maskBitsPerWord - 1 - firstIndex % maskBitsPerWord);
//    word <<= shift;
//    while (true) {
//        unsigned leadingZeros = std::__countl_zero(word);
//        if (leadingZeros == maskBitsPerWord) {
//            wordIndex -= 1;
//            if (wordIndex < 0) {
//                return true;
//            }
//            shift = 0;
//            word = mask[wordIndex];
//        } else {
//            shift += leadingZeros;
//            ASSUME(shift < maskBitsPerWord);
//            word <<= leadingZeros;
//            unsigned entryIndex = wordIndex * maskBitsPerWord + (maskBitsPerWord - 1 - shift);
//            NumericPart numericPart = __builtin_bswap32(arrayStart + static_cast<NumericPart>(entryIndex));
//            unsigned numericPartLen = fullKeyLen - nprefLen;
//            memcpy(keyOut + nprefLen, reinterpret_cast<uint8_t*>(&numericPart) + sizeof(NumericPart) - numericPartLen, numericPartLen);
//            if (!found_record_cb(fullKeyLen, getValD1(entryIndex), valLen)) {
//                return false;
//            }
//            shift += 1;
//            word <<= 1;
//        }
//    }
//}

void DenseNode::print() {
    printf("# DenseNode%d\n", tag() == Tag::Dense ? 1 : 2);
    printf("lower fence: ");
    printKey(getLowerFence());
    printf("\nupper fence: ");
    printKey(getUpperFence());
    uint8_t keyBuffer[fullKeyLen];
    printf("\n");
    for (unsigned i = 0; i < slotCount; ++i) {
        if (tag() == Tag::Dense ? isSlotPresent(i) : slots[i] != 0) {
            printf("%d: ", i);
            restoreKey(arrayStart, fullKeyLen, getLowerFence().data(), keyBuffer, i);
            printKey({keyBuffer + prefixLength, static_cast<uint16_t>(fullKeyLen - prefixLength)});
            printf("\n");
        }
    }
}

void DenseNode::insertSlotWithSpace(unsigned int i, std::span<uint8_t> payload) {
    spaceUsed += payload.size() + 2;
    dataOffset -= payload.size() + 2;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wassume"
    ASSUME(slotEndOffset() <= dataOffset);
#pragma clang diagnostic pop
    ASSUME(i < slotCount);
    storeUnaligned<uint16_t>(ptr() + dataOffset, payload.size());
    slots[i] = dataOffset;
    copySpan(getValD2(i), payload);
}

bool DenseNode::requestSpaceFor(unsigned int payloadLen) {
    if (dataOffset - slotEndOffset() >= payloadLen + 2) {
        return true;
    }
    if (slotEndOffset() + spaceUsed + payloadLen + 2 <= fencesOffset()) {
        uint8_t buffer[pageSizeLeaf];
        unsigned bufferOffset = fencesOffset();
        for (unsigned i = 0; i < slotCount; ++i) {
            if (slots[i]) {
                bufferOffset -= 2 + slotValLen(i);
                memcpy(buffer + bufferOffset, ptr() + slots[i], 2 + slotValLen(i));
                slots[i] = bufferOffset;
            }
        }
        memcpy(ptr() + bufferOffset, buffer + bufferOffset, fencesOffset() - bufferOffset);
        return true;
    }
    return false;
}

unsigned DenseNode::slotEndOffset() {
    return offsetof(DenseNode, slots) + 2 * slotCount;
}

unsigned DenseNode::slotValLen(unsigned int index) {
    return loadUnaligned<uint16_t>(ptr() + slots[index]);
}

bool DenseNode::isNumericRangeAnyLen(std::span<uint8_t> key) {
    for (unsigned i = prefixLength; i < computeNumericPrefixLength(fullKeyLen); ++i) {
        if (getLowerFence()[i] != (i < key.size() ? key[i] : 0))
            return false;
    }
    return true;
}