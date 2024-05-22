#pragma once


struct VmcBTreeNode;

struct VmcBTreeNodeHeader {
    static const unsigned underFullSize = (pageSize / 2) + (pageSize / 4);  // merge nodes more empty
    static const u64 noNeighbour = ~0ull;

    struct FenceKeySlot {
        u16 offset;
        u16 len;
    };

    bool dirty;
    union {
        PID upperInnerNode; // inner
        PID nextLeafNode = noNeighbour; // leaf
    };

    bool hasRightNeighbour() { return nextLeafNode != noNeighbour; }

    FenceKeySlot lowerFence = {0, 0};  // exclusive
    FenceKeySlot upperFence = {0, 0};  // inclusive

    bool hasLowerFence() { return !!lowerFence.len; };

    u16 count = 0;
    bool isLeaf;
    u16 spaceUsed = 0;
    u16 dataOffset = static_cast<u16>(pageSize);
    u16 prefixLen = 0;

    static const unsigned hintCount = 16;
    u32 hint[hintCount];
    u32 padding;

    VmcBTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}

    ~VmcBTreeNodeHeader() {}
};

struct MetaDataPage {
    bool dirty;
    PID roots[(pageSize - 8) / 8];

    PID getRoot(unsigned slot) { return roots[slot]; }
};


struct VmcBTreeNode : public VmcBTreeNodeHeader {
    struct Slot {
        u16 offset;
        u16 keyLen;
        u16 payloadLen;
        union {
            u32 head;
            u8 headBytes[4];
        };
    } __attribute__((packed));
    union {
        Slot slot[(pageSize - sizeof(VmcBTreeNodeHeader)) / sizeof(Slot)];  // grows from front
        u8 heap[pageSize - sizeof(VmcBTreeNodeHeader)];                // grows from back
    };

    static constexpr unsigned maxKVSize = ((pageSize - sizeof(VmcBTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

    VmcBTreeNode(bool isLeaf) : VmcBTreeNodeHeader(isLeaf) { dirty = true; }

    u8 *ptr() { return reinterpret_cast<u8 *>(this); }

    bool isInner() { return !isLeaf; }

    std::span<u8> getLowerFence() { return {ptr() + lowerFence.offset, lowerFence.len}; }

    std::span<u8> getUpperFence() { return {ptr() + upperFence.offset, upperFence.len}; }

    u8 *getPrefix() { return ptr() + lowerFence.offset; } // any key on page is ok

    unsigned freeSpace() { return dataOffset - (reinterpret_cast<u8 *>(slot + count) - ptr()); }

    unsigned freeSpaceAfterCompaction() {
        return pageSize - (reinterpret_cast<u8 *>(slot + count) - ptr()) - spaceUsed;
    }

    bool hasSpaceFor(unsigned keyLen, unsigned payloadLen);

    u8 *getKey(unsigned slotId) { return ptr() + slot[slotId].offset; }

    std::span<u8> getPayload(unsigned slotId) {
        return {ptr() + slot[slotId].offset + slot[slotId].keyLen, slot[slotId].payloadLen};
    }

    PID getChild(unsigned slotId);

    // How much space would inserting a new key of len "keyLen" require?
    unsigned spaceNeeded(unsigned keyLen, unsigned payloadLen);

    void makeHint();

    void updateHint(unsigned slotId);

    void searchHint(u32 keyHead, u16 &lowerOut, u16 &upperOut);

    // lower bound search, foundExactOut indicates if there is an exact match, returns slotId
    u16 lowerBound(std::span<u8> skey, bool &foundExactOut);

    // lowerBound wrapper ignoring exact match argument (for convenience)
    u16 lowerBound(std::span<u8> key);

    // insert key/value pair
    void insertInPage(std::span<u8> key, std::span<u8> payload);

    bool removeSlot(unsigned slotId);

    bool removeInPage(std::span<u8> key);

    void copyNode(VmcBTreeNodeHeader *dst, VmcBTreeNodeHeader *src);

    void compactify();

    // merge right node into this node
    bool mergeNodes(unsigned slotId, VmcBTreeNode *parent, VmcBTreeNode *right);

    // store key/value pair at slotId
    void storeKeyValue(u16 slotId, std::span<u8> skey, std::span<u8> payload);

    void copyKeyValueRange(VmcBTreeNode *dst, u16 dstSlot, u16 srcSlot, unsigned srcCount);

    void copyKeyValue(u16 srcSlot, VmcBTreeNode *dst, u16 dstSlot);

    void insertFence(FenceKeySlot &fk, std::span<u8> key);

    void setFences(std::span<u8> lower, std::span<u8> upper);

    void splitNode(VmcBTreeNode *parent, unsigned sepSlot, std::span<u8> sep);

    struct SeparatorInfo {
        unsigned len;      // len of new separator
        unsigned slot;     // slot at which we split
        bool isTruncated;  // if true, we truncate the separator taking len bytes from slot+1
    };

    unsigned commonPrefix(unsigned slotA, unsigned slotB);

    SeparatorInfo findSeparator(bool splitOrdered);

    void getSep(u8 *sepKeyOut, SeparatorInfo info);

    PID lookupInner(std::span<u8> key);
};


static const u64 metadataPageId = 0;

struct VmcBTree {
private:

    void trySplit(GuardX<VmcBTreeNode> &&node, GuardX<VmcBTreeNode> &&parent, std::span<u8> key, unsigned payloadLen);

    void ensureSpace(VmcBTreeNode *toSplit, std::span<u8> key, unsigned payloadLen);

public:
    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);

    unsigned slotId;
    std::atomic<bool> splitOrdered;

    VmcBTree(bool isInt);

    ~VmcBTree();

    GuardO<VmcBTreeNode> findLeafO(std::span<u8> key) {
        GuardO<MetaDataPage> meta(metadataPageId);
        GuardO<VmcBTreeNode> node(meta->getRoot(slotId), meta);
        meta.release();

        while (node->isInner())
            node = GuardO<VmcBTreeNode>(node->lookupInner(key), node);
        return node;
    }

    // point lookup, returns payload len on success, or -1 on failure
    int lookup(std::span<u8> key, u8 *payloadOut, unsigned payloadOutSize);

    template<class Fn>
    bool lookup(std::span<u8> key, Fn fn) {
        for (u64 repeatCounter = 0;; repeatCounter++) {
            try {
                GuardO<VmcBTreeNode> node = findLeafO(key);
                bool found;
                unsigned pos = node->lowerBound(key, found);
                if (!found)
                    return false;

                // key found
                fn(node->getPayload(pos));
                return true;
            } catch (const OLCRestartException &) { vmcache_yield(repeatCounter); }
        }
    }

    void insert(std::span<u8> key, std::span<u8> payload);

    bool remove(std::span<u8> key);

    template<class Fn>
    bool updateInPlace(std::span<u8> key, Fn fn) {
        for (u64 repeatCounter = 0;; repeatCounter++) {
            try {
                GuardO<VmcBTreeNode> node = findLeafO(key);
                bool found;
                unsigned pos = node->lowerBound(key, found);
                if (!found)
                    return false;

                {
                    GuardX<VmcBTreeNode> nodeLocked(std::move(node));
                    fn(nodeLocked->getPayload(pos));
                    return true;
                }
            } catch (const OLCRestartException &) { vmcache_yield(repeatCounter); }
        }
    }

    GuardS<VmcBTreeNode> findLeafS(std::span<u8> key) {
        for (u64 repeatCounter = 0;; repeatCounter++) {
            try {
                GuardO<MetaDataPage> meta(metadataPageId);
                GuardO<VmcBTreeNode> node(meta->getRoot(slotId), meta);
                meta.release();

                while (node->isInner())
                    node = GuardO<VmcBTreeNode>(node->lookupInner(key), node);

                return GuardS<VmcBTreeNode>(std::move(node));
            } catch (const OLCRestartException &) { vmcache_yield(repeatCounter); }
        }
    }

    template<class Fn>
    void scanAsc(std::span<u8> key, Fn fn) {
        GuardS<VmcBTreeNode> node = findLeafS(key);
        bool found;
        unsigned pos = node->lowerBound(key, found);
        for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
            if (pos < node->count) {
                if (!fn(*node.ptr, pos))
                    return;
                pos++;
            } else {
                if (!node->hasRightNeighbour())
                    return;
                pos = 0;
                node = GuardS<VmcBTreeNode>(node->nextLeafNode);
            }
        }
    }

    template<class Fn>
    void scanDesc(std::span<u8> key, Fn fn) {
        GuardS<VmcBTreeNode> node = findLeafS(key);
        bool exactMatch;
        int pos = node->lowerBound(key, exactMatch);
        if (pos == node->count) {
            pos--;
            exactMatch = true; // XXX:
        }
        for (u64 repeatCounter = 0;; repeatCounter++) { // XXX
            while (pos >= 0) {
                if (!fn(*node.ptr, pos, exactMatch))
                    return;
                pos--;
            }
            if (!node->hasLowerFence())
                return;
            node = findLeafS(node->getLowerFence());
            pos = node->count - 1;
        }
    }
};
