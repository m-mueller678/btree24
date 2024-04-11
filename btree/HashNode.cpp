#include "HashNode.hpp"
#include "AnyNode.hpp"
#include "common.hpp"

static __thread HashNode *sortNode;

struct SlotProxy {
    HashSlot slot;

    friend bool operator<(const SlotProxy &l, const SlotProxy &r) {
        uint8_t *lptr = sortNode->ptr() + l.slot.offset;
        uint8_t *rptr = sortNode->ptr() + r.slot.offset;
        return std::lexicographical_compare(lptr, lptr + l.slot.keyLen, rptr, rptr + r.slot.keyLen);
    }
};


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
    spaceUsed = upperFence.size() + lowerFence.size();
    dataOffset = pageSizeLeaf - spaceUsed - hashCapacity;
    hashOffset = dataOffset;
    this->hashCapacity = hashCapacity;
    this->lowerFenceLen = lowerFence.size();
    this->upperFenceLen = upperFence.size();
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

bool HashNode::insert(std::span<uint8_t> key, std::span<uint8_t> payload) {
    rangeOpCounter.point_op();
    assert(freeSpace() < pageSizeLeaf);
    ASSUME(key.size() >= prefixLength);
    validate();
    if (!requestSlotAndSpace(key.size() - prefixLength + payload.size())) {
        assert(freeSpace() < pageSizeLeaf);
        return false;
    }
    uint8_t hash = compute_hash(key.subspan(prefixLength, key.size() - prefixLength));
    int index = findIndex(key, hash);
    if (index < 0) {
        storeKeyValue(count, key, payload, hash);
        count += 1;
    } else {
        storeKeyValue(index, key, payload, hash);
        spaceUsed -= (key.size() - prefixLength + payload.size());
    }
    assert(freeSpace() < pageSizeLeaf);
    validate();
    return true;
}

unsigned int HashNode::freeSpace() {
    return dataOffset - (reinterpret_cast<uint8_t *>(slot + count) - ptr());
}

void HashNode::validate() {

#ifdef NDEBUG
    return;
#endif
    // space used
    unsigned used = upperFenceLen + lowerFenceLen;
    for (unsigned i = 0; i < count; ++i)
        used += slot[i].keyLen + slot[i].payloadLen;
    assert(used == spaceUsed);
    assert(lowerFenceLen == prefixLength && upperFenceLen > prefixLength || upperFenceLen == 0 ||
           lowerFenceLen > prefixLength && upperFenceLen > prefixLength &&
           getLowerFence()[prefixLength] < getUpperFence()[prefixLength]);
    sortNode = this;
    for (unsigned i = 1; i < sortedCount; ++i)
        assert(*(reinterpret_cast<SlotProxy *>(slot + (i - 1))) < *(reinterpret_cast<SlotProxy *>(slot + i)));

    {  // check hashes
        for (unsigned i = 0; i < count; ++i) {
            uint8_t h = compute_hash(getKey(i));
            assert(h == hashes()[i]);
        }
    }
}


unsigned HashNode::freeSpaceAfterCompaction() {
    return pageSizeLeaf - (reinterpret_cast<uint8_t *>(slot + count) - ptr()) - spaceUsed;
}

uint8_t HashNode::compute_hash(std::span<uint8_t> key) {
    uint8_t hash;
    std::hash<std::string_view> hasher;
    hash = hasher(std::string_view{reinterpret_cast<const char *>(key.data()), key.size()});
    return hash;
}


unsigned HashNode::estimateCapacity() {
    unsigned available = pageSizeLeaf - sizeof(HashNodeHeader) - upperFenceLen - lowerFenceLen;
    unsigned entrySpaceUse = spaceUsed - upperFenceLen - lowerFenceLen + count * sizeof(HashSlot);
    // equivalent to `available / (entrySpaceUse/count +1)`
    unsigned capacity = count == 0 ? pageSizeLeaf / 64 : available * count / (entrySpaceUse + count);
    ASSUME(capacity >= count);
    return capacity;
}


int HashNode::findIndex(std::span<uint8_t> key, uint8_t hash) {
    return findIndexSimd(key, hash);
}

typedef uint8_t HashSimdVecByte __attribute__((vector_size(hashSimdWidth)));
// this requires clang version >= 15
typedef bool HashSimdVecBool __attribute__((ext_vector_type(hashSimdWidth)));

inline HashSimdBitMask hashSimdEq(HashSimdVecByte *a, HashSimdVecByte *b) {
    HashSimdVecBool equality = __builtin_convertvector(*a == *b, HashSimdVecBool);
    HashSimdBitMask equality_bits;
    memcpy(&equality_bits, &equality, sizeof(HashSimdBitMask));
    return equality_bits;
}

int HashNode::findIndexSimd(std::span<uint8_t> key, uint8_t hash) {
    ASSUME(reinterpret_cast<uintptr_t>(this) % alignof(HashSimdVecByte) == 0);
    key = key.subspan(prefixLength, key.size() - prefixLength);
    int hashMisalign = hashOffset % alignof(HashSimdVecByte);
    auto hashes = this->hashes();
    HashSimdVecByte *haystack_ptr = reinterpret_cast<HashSimdVecByte *>(hashes.data() - hashMisalign);
    HashSimdVecByte needle = hash - HashSimdVecByte{};
    unsigned shift = hashMisalign;
    HashSimdBitMask matches = hashSimdEq(haystack_ptr, &needle) >> shift;
    unsigned shift_limit = shift + hashes.size();
    while (shift < shift_limit) {
        unsigned trailing_zeros = std::__countr_zero(matches);
        if (trailing_zeros == hashSimdWidth) {
            shift = shift - shift % hashSimdWidth + hashSimdWidth;
            if (shift >= shift_limit) {
                return -1;
            }
            haystack_ptr += 1;
            matches = hashSimdEq(haystack_ptr, &needle);
        } else {
            shift += trailing_zeros;
            matches >>= trailing_zeros;
            matches -= 1;
            if (shift >= shift_limit) {
                return -1;
            }
            unsigned elementIndex = shift - hashMisalign;
            if (slot[elementIndex].keyLen == key.size() && span_compare(getKey(elementIndex), key) == 0) {
                return elementIndex;
            }
        }
    }
    return -1;
}

void HashNode::copyKeyValueToBasic(unsigned srcSlot, BTreeNode *dst, unsigned dstSlot) {
    unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
    uint8_t buffer[fullLength];
    memcpy(buffer, getLowerFence().data(), prefixLength);
    const std::span<uint8_t> key = getKey(srcSlot);
    memcpy(buffer + prefixLength, key.data(), key.size());
    dst->storeKeyValue(dstSlot, {buffer, fullLength}, getPayload(srcSlot));
}

void HashNode::storeKeyValue(unsigned slotId, std::span<uint8_t> key, std::span<uint8_t> payload, uint8_t hash) {
    // slot
    key = key.subspan(prefixLength, key.size() - prefixLength);
    slot[slotId].keyLen = key.size();
    slot[slotId].payloadLen = payload.size();
    // key
    unsigned space = key.size() + payload.size();
    dataOffset -= space;
    spaceUsed += space;
    slot[slotId].offset = dataOffset;
    assert(getKey(slotId).data() >= reinterpret_cast<uint8_t *>(&slot[slotId]));
    copySpan(getKey(slotId), key);
    copySpan(getPayload(slotId), payload);
    hashes()[slotId] = hash;
}

std::span<uint8_t> HashNode::hashes() {
    return slice(hashOffset, count);
}

void HashNode::compactify(unsigned newHashCapacity) {
    unsigned should = freeSpaceAfterCompaction() - newHashCapacity;
    HashNode tmp;
    tmp.init(getLowerFence(), getUpperFence(), newHashCapacity, rangeOpCounter);
    tmp.count = count;
    copySpan(tmp.hashes(), hashes());
    memcpy(tmp.slot, slot, sizeof(HashSlot) * count);
    copyKeyValueRange(&tmp, 0, 0, count);
    tmp.sortedCount = sortedCount;
    assert(tmp.freeSpace() == should);
    *this = tmp;
}


void HashNode::copyKeyValueRange(HashNode *dst, unsigned dstSlot, unsigned srcSlot, unsigned srcCount) {
    assert(dstSlot + srcCount <= dst->count);
    if (prefixLength <= dst->prefixLength) {  // prefix grows
        unsigned diff = dst->prefixLength - prefixLength;
        for (unsigned i = 0; i < srcCount; i++) {
            unsigned newKeyLength = slot[srcSlot + i].keyLen - diff;
            unsigned space = newKeyLength + slot[srcSlot + i].payloadLen;
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            dst->slot[dstSlot + i].keyLen = newKeyLength;
            dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
            uint8_t *key = getKey(srcSlot + i).data() + diff;
            memcpy(dst->getKey(dstSlot + i).data(), key, space);
            dst->updateHash(dstSlot + i);
        }
    } else {
        for (unsigned i = 0; i < srcCount; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
    }
    assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t *>(dst->slot + dst->count));
}


bool HashNode::requestSlotAndSpace(unsigned kvSize) {
    if (count < hashCapacity && kvSize + sizeof(HashSlot) <= freeSpace())
        return true;  // avoid capacity estimate calculation
    unsigned onCompactifyCapacity = max(estimateCapacity(), count + 1);
    if (count < hashCapacity) {
        if (onCompactifyCapacity + kvSize + sizeof(HashSlot) > freeSpaceAfterCompaction()) {
            return false;
        }
    } else {
        unsigned hashGrowCapacity = onCompactifyCapacity;
        if (hashGrowCapacity + kvSize + sizeof(HashSlot) <= freeSpace()) {
            const std::span<uint8_t> oldHashes = hashes();
            dataOffset -= hashGrowCapacity;
            hashOffset = dataOffset;
            hashCapacity = hashGrowCapacity;
            copySpan(hashes(), oldHashes);
            return true;
        } else if (onCompactifyCapacity + kvSize + sizeof(HashSlot) > freeSpaceAfterCompaction()) {
            return false;
        }
    }
    // not worth compacting for a few more keys
    if (onCompactifyCapacity <= count + (unsigned) (count) * 3 / 128)
        return false;
    compactify(onCompactifyCapacity);
    return true;
}


void HashNode::updateHash(unsigned int i) {
    hashes()[i] = compute_hash(getKey(i));
}


unsigned HashNode::commonPrefix(unsigned slotA, unsigned slotB) {
    assert(slotA < count);
    assert(slotB < count);
    return commonPrefixLength(getKey(slotA), getKey(slotB));
}

// splits after sepSlot
SeparatorInfo HashNode::findSeparator() {
    ASSUME(count > 1);

    // find good separator slot
    unsigned lower = count / 2 - count / 32;
    unsigned upper = lower + count / 16;

    unsigned rangeCommonPrefix = commonPrefix(lower, upper);
    if (slot[lower].keyLen == rangeCommonPrefix) {
        return SeparatorInfo{prefixLength + rangeCommonPrefix, lower, false};
    }
    for (unsigned i = lower + 1;; ++i) {
        assert(i < upper + 1);
        if (getKey(i)[rangeCommonPrefix] != getKey(lower)[rangeCommonPrefix]) {
            if (slot[i].keyLen == rangeCommonPrefix + 1)
                return SeparatorInfo{prefixLength + rangeCommonPrefix + 1, i, false};
            else
                return SeparatorInfo{prefixLength + rangeCommonPrefix + 1, i - 1, true};
        }
    }
    ASSUME(false);
}


void HashNode::copyKeyValue(unsigned srcSlot, HashNode *dst, unsigned dstSlot) {
    unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
    uint8_t buffer[fullLength];
    memcpy(buffer, getLowerFence().data(), prefixLength);
    const std::span<uint8_t> key = getKey(srcSlot);
    memcpy(buffer + prefixLength, key.data(), key.size());
    dst->storeKeyValue(dstSlot, {buffer, fullLength}, getPayload(srcSlot),
                       compute_hash({buffer + dst->prefixLength, fullLength - dst->prefixLength}));
}


void HashNode::sort() {
    validate();
    if (sortedCount == count)
        return;
    // TODO could preserve hashes with some effort
    SlotProxy *slotProxy = reinterpret_cast<SlotProxy *>(slot);
    sortNode = this;
    std::sort(slotProxy + sortedCount, slotProxy + count);
    std::inplace_merge(slotProxy, slotProxy + sortedCount, slotProxy + count);
    for (unsigned i = 0; i < count; ++i) {
        updateHash(i);
    }
    sortedCount = count;
    validate();
}

void HashNode::splitNode(AnyNode *parent, unsigned sepSlot, std::span<std::uint8_t> sepKey) {
    if (enableHashAdapt) {
        bool goodHeads = hasGoodHeads();
        if (goodHeads) {
            rangeOpCounter.setGoodHeads();
            TODO_UNIMPL //return splitToBasic(parent, sepSlot, sepKey);
        } else if (!rangeOpCounter.isLowRange()) {
            TODO_UNIMPL //return splitToBasic(parent, sepSlot, sepKey);
        }
    }
    // split this node into nodeLeft and nodeRight
    assert(sepSlot > 0);
    auto nodeLeftAlloc = AnyNode::allocLeaf();
    HashNode *nodeLeft = &nodeLeftAlloc->_hash;
    unsigned capacity = estimateCapacity();
    nodeLeft->init(getLowerFence(), sepKey, capacity, rangeOpCounter);
    HashNode right;
    right.init(sepKey, getUpperFence(), capacity, rangeOpCounter);
    bool succ = parent->insertChild(sepKey, nodeLeftAlloc.pid);
    assert(succ);
    nodeLeft->count = sepSlot + 1;
    nodeLeft->sortedCount = nodeLeft->count;
    copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
    right.count = count - nodeLeft->count;
    right.sortedCount = right.count;
    copyKeyValueRange(&right, 0, nodeLeft->count, count - nodeLeft->count);
    nodeLeft->validate();
    right.validate();
    memcpy(this, &right, pageSizeLeaf);
}

void HashNode::getSep(uint8_t *sepKeyOut, SeparatorInfo info) {
    memcpy(sepKeyOut, getLowerFence().data(), prefixLength);
    memcpy(sepKeyOut + prefixLength, getKey(info.slot + info.isTruncated).data(), info.length - prefixLength);
}

bool HashNode::lookup(std::span<uint8_t> key, std::span<uint8_t> &valueOut) {
    rangeOpCounter.point_op();
    int index = findIndex(key, compute_hash(key.subspan(prefixLength, key.size() - prefixLength)));
    if (index >= 0) {
        auto pl = getPayload(index);
        if (pl.size() > maxKvSize)
            throw OLCRestartException();
        valueOut = {valueOut.data(), pl.size()};
        copySpan(valueOut, pl);
        return true;
    }
    return false;
}
