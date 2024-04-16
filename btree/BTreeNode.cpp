#include "BTreeNode.hpp"
#include "common.hpp"

#include "AnyNode.hpp"

void BTreeNode::print() {
    printf("# BTreeNode\n");
    printf("lower fence: ");
    printKey(getLowerFence());
    printf("\nupper fence: ");
    printKey(getUpperFence());
    printf("\n");
    for (unsigned i = 0; i < count; ++i) {
        printf("%d: ", i);
        printKey(getKey(i));
        if (isInner()) {
            printf("-> %p\n", reinterpret_cast<void *>(getChild(i)));
        } else {
            printf("\n");
        }
    }
    if (isInner()) {
        printf("upper -> %p\n", reinterpret_cast<void *>(upper));
    }
}

GuardX<AnyNode> BTreeNode::makeLeaf() {
    GuardX<AnyNode> r = AnyNode::allocLeaf();
    r->_basic_node.init(true, RangeOpCounter{});
    return r;
}

std::span<uint8_t> BTreeNode::getLowerFence() {
    return slice(lowerFence.offset, lowerFence.length);
}

std::span<uint8_t> BTreeNode::getUpperFence() {
    return slice(upperFence.offset, upperFence.length);
}

std::span<uint8_t> BTreeNode::slice(uint16_t offset, uint16_t len) {
    if (uint32_t(offset) + uint32_t(len) > pageSize)
        throw OLCRestartException{};
    return {ptr() + offset, ptr() + offset + len};
}


std::span<uint8_t> BTreeNode::getKey(unsigned slotId) {
    return slice(slot[slotId].offset, slot[slotId].keyLen);
}

std::span<uint8_t> BTreeNode::getPayload(unsigned slotId) {
    return slice(slot[slotId].offset + slot[slotId].keyLen, slot[slotId].payloadLen);
}

bool BTreeNode::isInner() {
    return ::isInner(tag());
}

bool BTreeNode::isLeaf() {
    return !isInner();
}

PID BTreeNode::getChild(unsigned slotId) {
    assert(isInner());
    auto pl = getPayload(slotId);
    if (pl.size() != sizeof(PID))
        throw OLCRestartException();
    return loadUnaligned<PID>(getPayload(slotId).data());
}

void BTreeNode::init(bool isLeaf, RangeOpCounter roc) {
    BTreeNodeHeader::init(isLeaf, roc);
}

void BTreeNodeHeader::init(bool isLeaf, RangeOpCounter roc) {
    set_tag(isLeaf ? Tag::Leaf : Tag::Inner);
    rangeOpCounter = roc;
    count = 0;
    spaceUsed = 0;
    dataOffset = (isLeaf ? pageSizeLeaf : pageSizeInner);
    lowerFence = {};
    upperFence = {};
    upper = 0;
}

uint8_t *BTreeNode::ptr() {
    return reinterpret_cast<uint8_t *>(this);
}


bool BTreeNode::insertChild(std::span<uint8_t> key, PID child) {
    return insert(key, std::span{reinterpret_cast<uint8_t *>(&child), sizeof(AnyNode *)});
}

bool BTreeNode::insert(std::span<uint8_t> key, std::span<uint8_t> payload) {
    assert(key.size() >= prefixLength);
    assert(span_compare(getPrefix(), key.subspan(0, prefixLength)) == 0);
    assert(span_compare(getLowerFence(), key) < 0);
    assert(span_compare(key, getUpperFence()) <= 0 || getUpperFence().empty());

    if (!requestSpaceFor(spaceNeeded(key.size(), payload.size()))) {
        AnyNode tmp;
        bool densify1 = enableDense && tag() == Tag::Leaf && key.size() - prefixLength == slot[0].keyLen &&
                        payload.size() == slot[0].payloadLen;
        bool densify2 = enableDense2 && tag() == Tag::Leaf && key.size() - prefixLength == slot[0].keyLen;
        if ((densify1 || densify2) && tmp._dense.try_densify(this)) {
            memcpy(this, &tmp, pageSizeLeaf);
            return this->any()->dense()->insert(key, payload);
        }
        return false;  // no space, insert fails
    }
    bool found;
    unsigned slotId = lowerBound(key, found);
    if (found) {
        storeKeyValue(slotId, key, payload);
    } else {
        memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
        storeKeyValue(slotId, key, payload);
        count++;
        updateHint(slotId);
    }
    return true;
}

unsigned BTreeNode::spaceNeeded(unsigned keyLength, unsigned payloadLength) {
    ASSUME(enablePrefix || prefixLength == 0);
    ASSUME(keyLength >=
           prefixLength);  // fence key logic makes it impossible to insert a key that is shorter than prefix
    return sizeof(Slot) + (keyLength - prefixLength) + payloadLength;
}

void BTreeNode::storeKeyValue(uint16_t slotId, std::span<uint8_t> key, std::span<uint8_t> payload) {
    auto prefixLength = this->prefixLength;
    ASSUME(enablePrefix || prefixLength == 0);
    key = key.subspan(prefixLength, key.size() - prefixLength);
    if (enableBasicHead) {
        slot[slotId].head[0] = head(key);
    }
    slot[slotId].keyLen = key.size();
    slot[slotId].payloadLen = payload.size();
    // key
    unsigned space = key.size() + payload.size();
    dataOffset -= space;
    spaceUsed += space;
    slot[slotId].offset = dataOffset;
    assert(getKey(slotId).data() >= reinterpret_cast<uint8_t *>(&slot[slotId + 1]));
    copySpan(getKey(slotId), key);
    copySpan(getPayload(slotId), payload);
}

unsigned BTreeNode::freeSpace() {
    return dataOffset - (reinterpret_cast<uint8_t *>(slot + count) - ptr());
}

unsigned BTreeNode::freeSpaceAfterCompaction() {
    return (isLeaf() ? pageSizeLeaf : pageSizeInner) - (reinterpret_cast<uint8_t *>(slot + count) - ptr()) - spaceUsed;
}

bool BTreeNode::requestSpaceFor(unsigned spaceNeeded) {
    if (spaceNeeded <= freeSpace())
        return true;
    if (spaceNeeded <= freeSpaceAfterCompaction()) {
        compactify();
        return true;
    }
    return false;
}

unsigned BTreeNode::lowerBound(std::span<uint8_t> key, bool &foundOut) {
    // validateHint();
    foundOut = false;

    // skip prefix
    uint16_t prefixLength = this->prefixLength;
    if (prefixLength > key.size())
        throw OLCRestartException();
    key = key.subspan(prefixLength, key.size() - prefixLength);

    // check hint
    unsigned lower = 0;
    unsigned upper = count;
    uint32_t keyHead = head(key);
    searchHint(keyHead, lower, upper);

    // binary search on remaining range
    while (lower < upper) {
        unsigned mid = ((upper - lower) / 2) + lower;
        if (enableBasicHead && keyHead < slot[mid].head[0]) {
            upper = mid;
        } else if (enableBasicHead && keyHead > slot[mid].head[0]) {
            lower = mid + 1;
        } else {  // head is equal, check full key
            auto candidate = getKey(mid);
            int cmp = memcmp(key.data(), candidate.data(), min(key.size(), candidate.size()));
            if (cmp < 0) {
                upper = mid;
            } else if (cmp > 0) {
                lower = mid + 1;
            } else {
                if (key.size() < candidate.size()) {  // key is shorter
                    upper = mid;
                } else if (key.size() > candidate.size()) {  // key is longer
                    lower = mid + 1;
                } else {
                    foundOut = true;
                    return mid;
                }
            }
        }
    }
    return lower;
}


unsigned BTreeNode::lowerBound(std::span<uint8_t> key) {
    bool discard;
    return lowerBound(key, discard);
}


void BTreeNode::makeHint() {
    unsigned dist = count / (hintCount + 1);
    for (unsigned i = 0; i < hintCount; i++)
        hint[i] = slot[dist * (i + 1)].head[0];
}

void BTreeNode::updateHint(unsigned slotId) {
    unsigned dist = count / (hintCount + 1);
    unsigned begin = 0;
    if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
        begin = (slotId / dist) - 1;
    for (unsigned i = begin; i < hintCount; i++)
        hint[i] = slot[dist * (i + 1)].head[0];
}


void BTreeNode::searchHint(uint32_t keyHead, unsigned &lowerOut, unsigned &upperOut) {
    if (hintCount == 0)
        return;
    if (count > hintCount * 2) {
        unsigned dist = upperOut / (hintCount + 1);
        unsigned pos, pos2;
        for (pos = 0; pos < hintCount; pos++)
            if (hint[pos] >= keyHead)
                break;
        for (pos2 = pos; pos2 < hintCount; pos2++)
            if (hint[pos2] != keyHead)
                break;
        lowerOut = pos * dist;
        if (pos2 < hintCount)
            upperOut = (pos2 + 1) * dist;
    }
}


void BTreeNode::compactify() {
    unsigned should = freeSpaceAfterCompaction();
    static_cast<void>(should);
    TmpBTreeNode tmp;
    tmp.node.init(isLeaf(), rangeOpCounter);
    tmp.node.setFences(getLowerFence(), getUpperFence());
    copyKeyValueRange(&tmp.node, 0, 0, count);
    tmp.node.upper = upper;
    memcpy(reinterpret_cast<char *>(this), &tmp.node, isLeaf() ? pageSizeLeaf : pageSizeInner);
    makeHint();
    assert(freeSpace() == should);
}

void BTreeNode::setFences(std::span<uint8_t> lower, std::span<uint8_t> upper) {
    assert(upper.empty() || std::lexicographical_compare(lower.begin(), lower.end(), upper.begin(), upper.end()));
    insertFence(lowerFence, lower);
    insertFence(upperFence, upper);
    for (prefixLength = 0; enablePrefix && (prefixLength < min(lower.size(), upper.size())) &&
                           (lower[prefixLength] == upper[prefixLength]); prefixLength++);
}

void BTreeNode::insertFence(BTreeNodeHeader::FenceKeySlot &slot, std::span<uint8_t> fence) {
    assert(freeSpace() >= fence.size());
    dataOffset -= fence.size();
    spaceUsed += fence.size();
    slot.offset = dataOffset;
    slot.length = fence.size();
    std::copy(fence.begin(), fence.end(), ptr() + dataOffset);
}


void BTreeNode::copyKeyValueRange(BTreeNode *dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount) {
    if (enablePrefix && prefixLength <= dst->prefixLength) {  // prefix grows
        unsigned diff = dst->prefixLength - prefixLength;
        for (unsigned i = 0; i < srcCount; i++) {
            unsigned newKeyLength = slot[srcSlot + i].keyLen - diff;
            unsigned space = newKeyLength + slot[srcSlot + i].payloadLen;
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            uint8_t *key = getKey(srcSlot + i).data() + diff;
            dst->slot[dstSlot + i].keyLen = newKeyLength;
            dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
            memcpy(dst->getKey(dstSlot + i).data(), key, space);
            if (enableBasicHead)
                dst->slot[dstSlot + i].head[0] = head({key, newKeyLength});
        }
    } else {
        for (unsigned i = 0; i < srcCount; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
    }
    dst->count += srcCount;
    assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t *>(dst->slot + dst->count));
}

void BTreeNode::copyKeyValue(uint16_t srcSlot, BTreeNode *dst, uint16_t dstSlot) {
    ASSUME(enablePrefix || prefixLength == 0);
    unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
    uint8_t keyBuffer[fullLength];
    const std::span<uint8_t> prefix = getPrefix();
    memcpy(keyBuffer, prefix.data(), prefix.size());
    const std::span<uint8_t> key = getKey(srcSlot);
    memcpy(keyBuffer + prefixLength, key.data(), key.size());
    dst->storeKeyValue(dstSlot, {keyBuffer, fullLength}, getPayload(srcSlot));
}

std::span<uint8_t> BTreeNode::getPrefix() {
    return slice(lowerFence.offset, prefixLength);
}

PID BTreeNode::lookupInner(std::span<uint8_t> key) {
    unsigned pos = lowerBound(key);
    if (pos == count)
        return upper;
    return getChild(pos);
}

// splits after sepSlot
SeparatorInfo BTreeNode::findSeparator() {
    constexpr bool USE_ORIGINAL = false;

    ASSUME(count > 1);
    ASSUME(enablePrefix || prefixLength == 0);
    if (isInner()) {
        // inner nodes are split in the middle
        unsigned slotId = count / 2 - 1;
        return SeparatorInfo{static_cast<unsigned>(prefixLength + slot[slotId].keyLen), slotId, false};
    }

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

void BTreeNode::getSep(uint8_t *sepKeyOut, SeparatorInfo info) {
    restoreKeyExclusive({sepKeyOut, info.length}, info.slot + info.isTruncated);
}


void BTreeNode::splitNode(AnyNode *parent, unsigned sepSlot, uint8_t *sepKey, unsigned sepLength) {
    // split this node into nodeLeft and nodeRight
    assert(sepSlot > 0);
    assert(sepSlot < ((isLeaf() ? pageSizeLeaf : pageSizeInner) / sizeof(BTreeNode *)));
    GuardX<AnyNode> nodeLeft;
    if (isLeaf()) {
        if (enableHashAdapt) {
            bool badHeads = hasBadHeads();
            if (badHeads) {
                rangeOpCounter.setBadHeads(rangeOpCounter.count);
            } else {
                rangeOpCounter.setGoodHeads();
            }
            if (badHeads && rangeOpCounter.isLowRange())
                return splitToHash(parent, sepSlot, {sepKey, sepLength});
        }
        nodeLeft = AnyNode::allocLeaf();
        nodeLeft->_basic_node.init(true, rangeOpCounter);
    } else {
        nodeLeft = AnyNode::allocInner();
        nodeLeft->_basic_node.init(false, rangeOpCounter);
    }
    auto left = nodeLeft->basic();
    left->setFences(getLowerFence(), {sepKey, sepLength});

    TmpBTreeNode tmp;
    tmp.node.init(isLeaf(), rangeOpCounter);
    BTreeNode *nodeRight = &tmp.node;
    nodeRight->setFences({sepKey, sepLength}, getUpperFence());
    bool succ = parent->insertChild({sepKey, sepLength}, nodeLeft.pid);
    ASSUME(succ);
    if (isLeaf()) {
        copyKeyValueRange(left, 0, 0, sepSlot + 1);
        copyKeyValueRange(nodeRight, 0, left->count, count - left->count);
    } else {
        // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
        copyKeyValueRange(left, 0, 0, sepSlot);
        copyKeyValueRange(nodeRight, 0, left->count + 1, count - left->count - 1);
        left->upper = getChild(left->count);
        nodeRight->upper = upper;
        // validate_child_fences();
    }
    left->makeHint();
    nodeRight->makeHint();
    memcpy(reinterpret_cast<char *>(this), nodeRight, isLeaf() ? pageSizeLeaf : pageSizeInner);
}

unsigned BTreeNode::commonPrefix(unsigned slotA, unsigned slotB) {
    assert(slotA < count);
    assert(slotB < count);
    return commonPrefixLength(getKey(slotA), getKey(slotB));
}


void BTreeNode::restoreKeyExclusive(std::span<uint8_t> keyOut, unsigned index) {
    ASSUME(enablePrefix || prefixLength == 0);
    auto key = getKey(index);
    ASSUME(keyOut.size() <= prefixLength + key.size());
    ASSUME(keyOut.size() > prefixLength);
    memcpy(keyOut.data(), getPrefix().data(), prefixLength);
    memcpy(keyOut.data() + prefixLength, getKey(index).data(), keyOut.size() - prefixLength);
}

bool BTreeNode::lookupLeaf(std::span<uint8_t> key, std::span<uint8_t> &valueOut) {
    bool found;
    unsigned pos = lowerBound(key, found);
    if (!found)
        return false;
    auto payload = getPayload(pos);
    if (payload.size() > maxKVSize)
        throw OLCRestartException();
    memcpy(valueOut.data(), payload.data(), payload.size());
    valueOut = {valueOut.data(), payload.size()};
    return true;
}

bool BTreeNode::hasBadHeads() {
    unsigned threshold = count / 16;
    unsigned collisionCount = 0;
    for (unsigned i = 1; i < count; ++i) {
        if (slot[i - 1].head[0] == slot[i].head[0]) {
            collisionCount += 1;
            if (collisionCount > threshold)
                break;
        }
    }
    return collisionCount > threshold;
}


void BTreeNode::splitToHash(AnyNode *parent, unsigned sepSlot, std::span<uint8_t> sepKey) {
    auto nodeLeft = AnyNode::allocLeaf();
    auto leftHash = &nodeLeft->_hash;
    unsigned capacity = count;
    leftHash->init(getLowerFence(), sepKey, capacity, rangeOpCounter);
    HashNode right;
    right.init(sepKey, getUpperFence(), capacity, rangeOpCounter);
    bool succ = parent->insertChild(sepKey, nodeLeft.pid);
    assert(succ);
    copyKeyValueRangeToHash(leftHash, 0, 0, sepSlot + 1);
    copyKeyValueRangeToHash(&right, 0, leftHash->count, count - leftHash->count);
    leftHash->sortedCount = leftHash->count;
    right.sortedCount = right.count;
    leftHash->validate();
    right.validate();
    memcpy(this, &right, pageSizeLeaf);
}


void BTreeNode::copyKeyValueRangeToHash(HashNode *dst, unsigned dstSlot, unsigned srcSlot, unsigned srcCount) {
    for (unsigned i = 0; i < srcCount; i++) {
        unsigned fullLength = slot[srcSlot + i].keyLen + prefixLength;
        uint8_t key[fullLength];
        memcpy(key, getLowerFence().data(), prefixLength);
        memcpy(key + prefixLength, getKey(srcSlot + i).data(), slot[srcSlot + i].keyLen);
        dst->storeKeyValue(dstSlot + i, {key, fullLength}, getPayload(srcSlot + i),
                           HashNode::compute_hash({key + dst->prefixLength, fullLength - dst->prefixLength}));
    }
    dst->count += srcCount;
    assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t *>(dst->slot + dst->count));
}

bool BTreeNode::range_lookup(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                             const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
    TODO_UNIMPL
}
