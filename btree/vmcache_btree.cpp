#include <atomic>
#include <algorithm>
#include <cassert>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>
#include <vector>
#include <span>

#include <errno.h>
#include <libaio.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>
#include "vmache.hpp"
#include "vmcache_btree.hpp"

using namespace std;

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef u64 PID; // page id type

//---------------------------------------------------------------------------

static unsigned min(unsigned a, unsigned b) {
    return a < b ? a : b;
}

template<class T>
static T loadUnaligned(void *p) {
    T x;
    memcpy(&x, p, sizeof(T));
    return x;
}

// Get order-preserving head of key (assuming little endian)
static u32 head(u8 *key, unsigned keyLen) {
    switch (keyLen) {
        case 0:
            return 0;
        case 1:
            return static_cast<u32>(key[0]) << 24;
        case 2:
            return static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key))) << 16;
        case 3:
            return (static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key))) << 16) |
                   (static_cast<u32>(key[2]) << 8);
        default:
            return __builtin_bswap32(loadUnaligned<u32>(key));
    }
}

static_assert(sizeof(VmcBTreeNode) == pageSize, "btree node size problem");

static unsigned btreeslotcounter = 0;

VmcBTree::VmcBTree(bool isInt) : splitOrdered(false) {
    GuardX<MetaDataPage> page(metadataPageId);
    AllocGuard<VmcBTreeNode> rootNode(true);
    slotId = btreeslotcounter++;
    page->roots[slotId] = rootNode.pid();
}

VmcBTree::~VmcBTree() {}

void VmcBTree::trySplit(GuardX<VmcBTreeNode> &&node, GuardX<VmcBTreeNode> &&parent, span<u8> key, unsigned payloadLen) {

    // create new root if necessary
    if (parent.pid() == metadataPageId) {
        MetaDataPage *metaData = reinterpret_cast<MetaDataPage *>(parent.ptr);
        AllocGuard<VmcBTreeNode> newRoot(false);
        newRoot->upperInnerNode = node.pid();
        metaData->roots[slotId] = newRoot.pid();
        parent = std::move(newRoot);
    }

    // split
    VmcBTreeNode::SeparatorInfo sepInfo = node->findSeparator(splitOrdered.load());
    u8 sepKey[sepInfo.len];
    node->getSep(sepKey, sepInfo);

    if (parent->hasSpaceFor(sepInfo.len, sizeof(PID))) {  // is there enough space in the parent for the separator?
        node->splitNode(parent.ptr, sepInfo.slot, {sepKey, sepInfo.len});
        return;
    }

    // must split parent to make space for separator, restart from root to do this
    node.release();
    VmcBTreeNode *parent_ptr = parent.ptr;
    parent.release();
    ensureSpace(parent_ptr, {sepKey, sepInfo.len}, sizeof(PID));
}

void VmcBTree::ensureSpace(VmcBTreeNode *toSplit, span<u8> key, unsigned payloadLen) {
    assert(toSplit);
    for (u64 repeatCounter = 0;; repeatCounter++) {
        try {
            GuardO<VmcBTreeNode> parent(metadataPageId);
            GuardO<VmcBTreeNode> node(reinterpret_cast<MetaDataPage *>(parent.ptr)->getRoot(slotId), parent);

            while (node->isInner() && (node.ptr != toSplit)) {
                parent = std::move(node);
                node = GuardO<VmcBTreeNode>(parent->lookupInner(key), parent);
            }
            if (node.ptr == toSplit) {
                if (node->hasSpaceFor(key.size(), payloadLen))
                    return; // someone else did split concurrently
                GuardX<VmcBTreeNode> parentLocked(std::move(parent));
                GuardX<VmcBTreeNode> nodeLocked(std::move(node));
                trySplit(std::move(nodeLocked), std::move(parentLocked), key, payloadLen);
            }
            return;
        } catch (const OLCRestartException &) { vmcache_yield(repeatCounter); }
    }
}

void VmcBTree::insert(span<u8> key, span<u8> payload) {
    assert((key.size() + payload.size()) <= VmcBTreeNode::maxKVSize);

    for (u64 repeatCounter = 0;; repeatCounter++) {
        try {
            GuardO<VmcBTreeNode> parent(metadataPageId);
            GuardO<VmcBTreeNode> node(reinterpret_cast<MetaDataPage *>(parent.ptr)->getRoot(slotId), parent);

            while (node->isInner()) {
                parent = std::move(node);
                node = GuardO<VmcBTreeNode>(parent->lookupInner(key), parent);
            }

            if (node->hasSpaceFor(key.size(), payload.size())) {
                // only lock leaf
                GuardX<VmcBTreeNode> nodeLocked(std::move(node));
                parent.release();
                nodeLocked->insertInPage(key, payload);
                return; // success
            }

            // lock parent and leaf
            GuardX<VmcBTreeNode> parentLocked(std::move(parent));
            GuardX<VmcBTreeNode> nodeLocked(std::move(node));
            trySplit(std::move(nodeLocked), std::move(parentLocked), key, payload.size());
            // insert hasn't happened, restart from root
        } catch (const OLCRestartException &) { vmcache_yield(repeatCounter); }
    }
}

bool VmcBTree::remove(span<u8> key) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
        try {
            GuardO<VmcBTreeNode> parent(metadataPageId);
            GuardO<VmcBTreeNode> node(reinterpret_cast<MetaDataPage *>(parent.ptr)->getRoot(slotId), parent);

            u16 pos;
            while (node->isInner()) {
                pos = node->lowerBound(key);
                PID nextPage = (pos == node->count) ? node->upperInnerNode : node->getChild(pos);
                parent = std::move(node);
                node = GuardO<VmcBTreeNode>(nextPage, parent);
            }

            bool found;
            unsigned slotId = node->lowerBound(key, found);
            if (!found)
                return false;

            unsigned sizeEntry = node->slot[slotId].keyLen + node->slot[slotId].payloadLen;
            if ((node->freeSpaceAfterCompaction() + sizeEntry >= VmcBTreeNodeHeader::underFullSize) &&
                (parent.pid() != metadataPageId) && (parent->count >= 2) && ((pos + 1) < parent->count)) {
                // underfull
                GuardX<VmcBTreeNode> parentLocked(std::move(parent));
                GuardX<VmcBTreeNode> nodeLocked(std::move(node));
                GuardX<VmcBTreeNode> rightLocked(parentLocked->getChild(pos + 1));
                nodeLocked->removeSlot(slotId);
                if (rightLocked->freeSpaceAfterCompaction() >= VmcBTreeNodeHeader::underFullSize) {
                    if (nodeLocked->mergeNodes(pos, parentLocked.ptr, rightLocked.ptr)) {
                    }
                }
            } else {
                GuardX<VmcBTreeNode> nodeLocked(std::move(node));
                parent.release();
                nodeLocked->removeSlot(slotId);
            }
            return true;
        } catch (const OLCRestartException &) { vmcache_yield(repeatCounter); }
    }
}

int VmcBTree::lookup(std::span<u8> key, u8 *payloadOut, unsigned int payloadOutSize) {
    for (u64 repeatCounter = 0;; repeatCounter++) {
        try {
            GuardO<VmcBTreeNode> node = findLeafO(key);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
                return -1;

            // key found, copy payload
            memcpy(payloadOut, node->getPayload(pos).data(), min(node->slot[pos].payloadLen, payloadOutSize));
            return node->slot[pos].payloadLen;
        } catch (const OLCRestartException &) { vmcache_yield(repeatCounter); }
    }
}

void VmcBTree::lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback) {
    lookup(key, callback);
}

void VmcBTree::insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload) {
    insert(key, payload);
}

void VmcBTree::range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                                const function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
    VmcBTreeNode *currentNode = nullptr;
    scanAsc(key, [&](VmcBTreeNode &node, unsigned slot) {
        if (&node != currentNode) {
            memcpy(keyOutBuffer, node.getPrefix(), node.prefixLen);
            currentNode = &node;
        }
        memcpy(keyOutBuffer + node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
        return found_record_cb(node.prefixLen + node.slot[slot].keyLen, node.getPayload(slot));
    });
}

typedef u64 KeyType;

void handleSEGFAULT(int signo, siginfo_t *info, void *extra) {
    void *page = info->si_addr;
    if (bm.isValidPtr(page)) {
        cerr << "segfault restart " << bm.toPID(page) << endl;
        throw OLCRestartException();
    } else {
        cerr << "segfault " << page << endl;
        _exit(1);
    }
}

template<class Record>
struct vmcacheAdapter {
    VmcBTree tree;

public:
    void scan(const typename Record::Key &key,
              const std::function<bool(const typename Record::Key &, const Record &)> &found_record_cb,
              std::function<void()> reset_if_scan_failed_cb) {
        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        u8 kk[Record::maxFoldLength()];
        tree.scanAsc({k, l}, [&](VmcBTreeNode &node, unsigned slot) {
            memcpy(kk, node.getPrefix(), node.prefixLen);
            memcpy(kk + node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
            typename Record::Key typedKey;
            Record::unfoldKey(kk, typedKey);
            return found_record_cb(typedKey, *reinterpret_cast<const Record *>(node.getPayload(slot).data()));
        });
    }

    // -------------------------------------------------------------------------------------
    void scanDesc(const typename Record::Key &key,
                  const std::function<bool(const typename Record::Key &, const Record &)> &found_record_cb,
                  std::function<void()> reset_if_scan_failed_cb) {
        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        u8 kk[Record::maxFoldLength()];
        bool first = true;
        tree.scanDesc({k, l}, [&](VmcBTreeNode &node, unsigned slot, bool exactMatch) {
            if (first) { // XXX: hack
                first = false;
                if (!exactMatch)
                    return true;
            }
            memcpy(kk, node.getPrefix(), node.prefixLen);
            memcpy(kk + node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
            typename Record::Key typedKey;
            Record::unfoldKey(kk, typedKey);
            return found_record_cb(typedKey, *reinterpret_cast<const Record *>(node.getPayload(slot).data()));
        });
    }

    // -------------------------------------------------------------------------------------
    void insert(const typename Record::Key &key, const Record &record) {
        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        tree.insert({k, l}, {(u8 *) (&record), sizeof(Record)});
    }

    // -------------------------------------------------------------------------------------
    template<class Fn>
    void lookup1(const typename Record::Key &key, Fn fn) {
        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        bool succ = tree.lookup({k, l}, [&](span<u8> payload) {
            fn(*reinterpret_cast<const Record *>(payload.data()));
        });
        assert(succ);
    }

    // -------------------------------------------------------------------------------------
    template<class Fn>
    void update1(const typename Record::Key &key, Fn fn) {
        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        tree.updateInPlace({k, l}, [&](span<u8> payload) {
            fn(*reinterpret_cast<Record *>(payload.data()));
        });
    }

    // -------------------------------------------------------------------------------------
    // Returns false if the record was not found
    bool erase(const typename Record::Key &key) {
        u8 k[Record::maxFoldLength()];
        u16 l = Record::foldKey(k, key);
        return tree.remove({k, l});
    }

    // -------------------------------------------------------------------------------------
    template<class Field>
    Field lookupField(const typename Record::Key &key, Field Record::*f) {
        Field value;
        lookup1(key, [&](const Record &r) { value = r.*f; });
        return value;
    }

    u64 count() {
        u64 cnt = 0;
        tree.scanAsc({(u8 *) nullptr, 0}, [&](VmcBTreeNode &node, unsigned slot) {
            cnt++;
            return true;
        });
        return cnt;
    }
};

u16 VmcBTreeNode::lowerBound(std::span<u8> skey, bool &foundExactOut) {
    foundExactOut = false;

    // check prefix
    int cmp = memcmp(skey.data(), getPrefix(), min(skey.size(), prefixLen));
    if (cmp < 0) // key is less than prefix
        return 0;
    if (cmp > 0) // key is greater than prefix
        return count;
    if (skey.size() < prefixLen) // key is equal but shorter than prefix
        return 0;
    u8 *key = skey.data() + prefixLen;
    unsigned keyLen = skey.size() - prefixLen;

    // check hint
    u16 lower = 0;
    u16 upper = count;
    u32 keyHead = head(key, keyLen);
    searchHint(keyHead, lower, upper);

    // binary search on remaining range
    while (lower < upper) {
        u16 mid = ((upper - lower) / 2) + lower;
        if (keyHead < slot[mid].head) {
            upper = mid;
        } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
        } else { // head is equal, check full key
            int cmp = memcmp(key, getKey(mid), min(keyLen, slot[mid].keyLen));
            if (cmp < 0) {
                upper = mid;
            } else if (cmp > 0) {
                lower = mid + 1;
            } else {
                if (keyLen < slot[mid].keyLen) { // key is shorter
                    upper = mid;
                } else if (keyLen > slot[mid].keyLen) { // key is longer
                    lower = mid + 1;
                } else {
                    foundExactOut = true;
                    return mid;
                }
            }
        }
    }
    return lower;
}

u16 VmcBTreeNode::lowerBound(std::span<u8> key) {
    bool ignore;
    return lowerBound(key, ignore);
}

void VmcBTreeNode::insertInPage(std::span<u8> key, std::span<u8> payload) {
    unsigned needed = spaceNeeded(key.size(), payload.size());
    if (needed > freeSpace()) {
        assert(needed <= freeSpaceAfterCompaction());
        compactify();
    }
    bool found;
    unsigned slotId = lowerBound(key, found);
    if (found) {
        spaceUsed -= slot[slotId].payloadLen + slot[slotId].keyLen;
    } else {
        memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
        count++;
    }
    storeKeyValue(slotId, key, payload);
    if (!found) {
        updateHint(slotId);
    }
}

bool VmcBTreeNode::removeSlot(unsigned int slotId) {
    spaceUsed -= slot[slotId].keyLen;
    spaceUsed -= slot[slotId].payloadLen;
    memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
    count--;
    makeHint();
    return true;
}

bool VmcBTreeNode::removeInPage(std::span<u8> key) {
    bool found;
    unsigned slotId = lowerBound(key, found);
    if (!found)
        return false;
    return removeSlot(slotId);
}

void VmcBTreeNode::copyNode(VmcBTreeNodeHeader *dst, VmcBTreeNodeHeader *src) {
    u64 ofs = offsetof(VmcBTreeNodeHeader, upperInnerNode);
    memcpy(reinterpret_cast<u8 *>(dst) + ofs, reinterpret_cast<u8 *>(src) + ofs, sizeof(VmcBTreeNode) - ofs);
}

void VmcBTreeNode::compactify() {
    unsigned should = freeSpaceAfterCompaction();
    static_cast<void>(should);
    VmcBTreeNode tmp(isLeaf);
    tmp.setFences(getLowerFence(), getUpperFence());
    copyKeyValueRange(&tmp, 0, 0, count);
    tmp.upperInnerNode = upperInnerNode;
    copyNode(this, &tmp);
    makeHint();
    assert(freeSpace() == should);
}

bool VmcBTreeNode::mergeNodes(unsigned int slotId, VmcBTreeNode *parent, VmcBTreeNode *right) {
    if (!isLeaf)
        // TODO: implement inner merge
        return true;

    assert(right->isLeaf);
    assert(parent->isInner());
    VmcBTreeNode tmp(isLeaf);
    tmp.setFences(getLowerFence(), right->getUpperFence());
    unsigned leftGrow = (prefixLen - tmp.prefixLen) * count;
    unsigned rightGrow = (right->prefixLen - tmp.prefixLen) * right->count;
    unsigned spaceUpperBound =
            spaceUsed + right->spaceUsed + (reinterpret_cast<u8 *>(slot + count + right->count) - ptr()) + leftGrow +
            rightGrow;
    if (spaceUpperBound > pageSize)
        return false;
    copyKeyValueRange(&tmp, 0, 0, count);
    right->copyKeyValueRange(&tmp, count, 0, right->count);
    PID pid = bm.toPID(this);
    memcpy(parent->getPayload(slotId + 1).data(), &pid, sizeof(PID));
    parent->removeSlot(slotId);
    tmp.makeHint();
    tmp.nextLeafNode = right->nextLeafNode;

    copyNode(this, &tmp);
    return true;
}

void VmcBTreeNode::storeKeyValue(u16 slotId, std::span<u8> skey, std::span<u8> payload) {
    // slot
    u8 *key = skey.data() + prefixLen;
    unsigned keyLen = skey.size() - prefixLen;
    slot[slotId].head = head(key, keyLen);
    slot[slotId].keyLen = keyLen;
    slot[slotId].payloadLen = payload.size();
    // key
    unsigned space = keyLen + payload.size();
    dataOffset -= space;
    spaceUsed += space;
    slot[slotId].offset = dataOffset;
    assert(getKey(slotId) >= reinterpret_cast<u8 *>(&slot[slotId]));
    memcpy(getKey(slotId), key, keyLen);
    memcpy(getPayload(slotId).data(), payload.data(), payload.size());
}

void VmcBTreeNode::copyKeyValueRange(VmcBTreeNode *dst, u16 dstSlot, u16 srcSlot, unsigned int srcCount) {
    if (prefixLen <= dst->prefixLen) {  // prefix grows
        unsigned diff = dst->prefixLen - prefixLen;
        for (unsigned i = 0; i < srcCount; i++) {
            unsigned newKeyLen = slot[srcSlot + i].keyLen - diff;
            unsigned space = newKeyLen + slot[srcSlot + i].payloadLen;
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            u8 *key = getKey(srcSlot + i) + diff;
            memcpy(dst->getKey(dstSlot + i), key, space);
            dst->slot[dstSlot + i].head = head(key, newKeyLen);
            dst->slot[dstSlot + i].keyLen = newKeyLen;
            dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
        }
    } else {
        for (unsigned i = 0; i < srcCount; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
    }
    dst->count += srcCount;
    assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<u8 *>(dst->slot + dst->count));
}

void VmcBTreeNode::copyKeyValue(u16 srcSlot, VmcBTreeNode *dst, u16 dstSlot) {
    unsigned fullLen = slot[srcSlot].keyLen + prefixLen;
    u8 key[fullLen];
    memcpy(key, getPrefix(), prefixLen);
    memcpy(key + prefixLen, getKey(srcSlot), slot[srcSlot].keyLen);
    dst->storeKeyValue(dstSlot, {key, fullLen}, getPayload(srcSlot));
}

void VmcBTreeNode::insertFence(VmcBTreeNodeHeader::FenceKeySlot &fk, std::span<u8> key) {
    assert(freeSpace() >= key.size());
    dataOffset -= key.size();
    spaceUsed += key.size();
    fk.offset = dataOffset;
    fk.len = key.size();
    memcpy(ptr() + dataOffset, key.data(), key.size());
}

void VmcBTreeNode::setFences(std::span<u8> lower, std::span<u8> upper) {
    insertFence(lowerFence, lower);
    insertFence(upperFence, upper);
    for (prefixLen = 0;
         (prefixLen < min(lower.size(), upper.size())) && (lower[prefixLen] == upper[prefixLen]); prefixLen++);
}

void VmcBTreeNode::splitNode(VmcBTreeNode *parent, unsigned int sepSlot, std::span<u8> sep) {
    assert(sepSlot > 0);
    assert(sepSlot < (pageSize / sizeof(PID)));

    VmcBTreeNode tmp(isLeaf);
    VmcBTreeNode *nodeLeft = &tmp;

    AllocGuard<VmcBTreeNode> newNode(isLeaf);
    VmcBTreeNode *nodeRight = newNode.ptr;

    nodeLeft->setFences(getLowerFence(), sep);
    nodeRight->setFences(sep, getUpperFence());

    PID leftPID = bm.toPID(this);
    u16 oldParentSlot = parent->lowerBound(sep);
    if (oldParentSlot == parent->count) {
        assert(parent->upperInnerNode == leftPID);
        parent->upperInnerNode = newNode.pid();
    } else {
        assert(parent->getChild(oldParentSlot) == leftPID);
        PID pid = newNode.pid();
        memcpy(parent->getPayload(oldParentSlot).data(), &pid, sizeof(PID));
    }
    parent->insertInPage(sep, {reinterpret_cast<u8 *>(&leftPID), sizeof(PID)});

    if (isLeaf) {
        copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
        copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
        nodeLeft->nextLeafNode = newNode.pid();
        nodeRight->nextLeafNode = this->nextLeafNode;
    } else {
        // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
        copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
        copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
        nodeLeft->upperInnerNode = getChild(nodeLeft->count);
        nodeRight->upperInnerNode = upperInnerNode;
    }
    nodeLeft->makeHint();
    nodeRight->makeHint();
    copyNode(this, nodeLeft);
}

unsigned VmcBTreeNode::commonPrefix(unsigned int slotA, unsigned int slotB) {
    assert(slotA < count);
    unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
    u8 *a = getKey(slotA), *b = getKey(slotB);
    unsigned i;
    for (i = 0; i < limit; i++)
        if (a[i] != b[i])
            break;
    return i;
}

VmcBTreeNode::SeparatorInfo VmcBTreeNode::findSeparator(bool splitOrdered) {
    assert(count > 1);
    if (isInner()) {
        // inner nodes are split in the middle
        unsigned slotId = count / 2;
        return SeparatorInfo{static_cast<unsigned>(prefixLen + slot[slotId].keyLen), slotId, false};
    }

    // find good separator slot
    unsigned bestPrefixLen, bestSlot;

    if (splitOrdered) {
        bestSlot = count - 2;
    } else if (count > 16) {
        unsigned lower = (count / 2) - (count / 16);
        unsigned upper = (count / 2);

        bestPrefixLen = commonPrefix(lower, 0);
        bestSlot = lower;

        if (bestPrefixLen != commonPrefix(upper - 1, 0))
            for (bestSlot = lower + 1; (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLen); bestSlot++);
    } else {
        bestSlot = (count - 1) / 2;
    }


    // try to truncate separator
    unsigned common = commonPrefix(bestSlot, bestSlot + 1);
    if ((bestSlot + 1 < count) && (slot[bestSlot].keyLen > common) && (slot[bestSlot + 1].keyLen > (common + 1)))
        return SeparatorInfo{prefixLen + common + 1, bestSlot, true};

    return SeparatorInfo{static_cast<unsigned>(prefixLen + slot[bestSlot].keyLen), bestSlot, false};
}

void VmcBTreeNode::getSep(u8 *sepKeyOut, VmcBTreeNode::SeparatorInfo info) {
    memcpy(sepKeyOut, getPrefix(), prefixLen);
    memcpy(sepKeyOut + prefixLen, getKey(info.slot + info.isTruncated), info.len - prefixLen);
}

PID VmcBTreeNode::lookupInner(std::span<u8> key) {
    unsigned pos = lowerBound(key);
    if (pos == count)
        return upperInnerNode;
    return getChild(pos);
}

bool VmcBTreeNode::hasSpaceFor(unsigned int keyLen, unsigned int payloadLen) {
    return spaceNeeded(keyLen, payloadLen) <= freeSpaceAfterCompaction();
}

PID VmcBTreeNode::getChild(unsigned int slotId) { return loadUnaligned<PID>(getPayload(slotId).data()); }

unsigned VmcBTreeNode::spaceNeeded(unsigned int keyLen, unsigned int payloadLen) {
    return sizeof(Slot) + (keyLen - prefixLen) + payloadLen;
}

void VmcBTreeNode::makeHint() {
    unsigned dist = count / (hintCount + 1);
    for (unsigned i = 0; i < hintCount; i++)
        hint[i] = slot[dist * (i + 1)].head;
}

void VmcBTreeNode::updateHint(unsigned int slotId) {
    unsigned dist = count / (hintCount + 1);
    unsigned begin = 0;
    if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
        begin = (slotId / dist) - 1;
    for (unsigned i = begin; i < hintCount; i++)
        hint[i] = slot[dist * (i + 1)].head;
}

void VmcBTreeNode::searchHint(u32 keyHead, u16 &lowerOut, u16 &upperOut) {
    if (count > hintCount * 2) {
        u16 dist = upperOut / (hintCount + 1);
        u16 pos, pos2;
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
