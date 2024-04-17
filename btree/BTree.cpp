#include "BTree.hpp"
#include "config.hpp"
#include "Tag.hpp"
#include "HashNode.hpp"
#include "BTreeNode.hpp"
#include "DenseNode.hpp"
#include "AnyNode.hpp"
#include "common.hpp"


struct MetaDataPage : public TagAndDirty {
    PID root;

    MetaDataPage(PID root) : root(root) {}
};


BTree::~BTree() {
    static std::atomic<uint32_t> TREES_DESTROYED = 0;
    if (TREES_DESTROYED.fetch_add(1) > 1) {
        abort();
    }
}

// take isInt to have same interface as in memory structures, but ignore it.
BTree::BTree(bool isInt) {
    auto root = (enableHash && !enableHashAdapt) ? HashNode::makeRootLeaf() : BTreeNode::makeLeaf();
    auto metadata = GuardX<MetaDataPage>::alloc();
    metadata->root = root.pid;
    this->metadataPid = metadata.pid;
#ifndef NDEBUG
    // prevent print from being optimized out. It is otherwise never called, but nice for debugging
    if (getenv("oMEeHAobn4")) {
        root->print();
    }
#endif
}

void BTree::insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload) {
    assert((key.size() + payload.size()) <= BTreeNode::maxKVSize);
    while (true) {
        try {
            GuardO<AnyNode> parent{metadataPid};
            GuardO<AnyNode> node(reinterpret_cast<MetaDataPage *>(parent.ptr)->root, parent);

            while (node->isAnyInner()) {
                parent = std::move(node);
                node = GuardO<AnyNode>(parent->lookupInner(key), parent);
            }

            parent.checkVersionAndRestart();
            GuardX<AnyNode> nodeLocked{std::move(node)};
            while (true) {
                switch (nodeLocked->tag()) {
                    case Tag::Leaf: {
                        if (nodeLocked->basic()->insert(key, payload)) {
                            parent.release_ignore();
                            return;
                        }
                        break;
                    }
                    case Tag::Dense:
                    case Tag::Dense2: {
                        if (nodeLocked->dense()->insert(key, payload)) {
                            parent.release_ignore();
                            return;
                        }
                        break;
                    }
                    case Tag::Hash: {
                        nodeLocked->hash()->rangeOpCounter.point_op();
                        if (nodeLocked->hash()->rangeOpCounter.shouldConvertBasic() &&
                            nodeLocked->hash()->tryConvertToBasic())
                            continue;
                        if (nodeLocked->hash()->insert(key, payload)) {
                            parent.release_ignore();
                            return;
                        }
                        break;
                    }
                    default:
                        ASSUME(false);
                }
                break;
            }

            GuardX<AnyNode> parentLocked{std::move(parent)};
            trySplit(std::move(nodeLocked), std::move(parentLocked), key);
            // insert hasn't happened, restart from root
        } catch (const OLCRestartException &) { yield(); }
    }
}


void BTree::trySplit(GuardX<AnyNode> node, GuardX<AnyNode> parent, std::span<uint8_t> key) {
    // create new root if necessary
    if (parent.pid == metadataPid) {
        MetaDataPage *metaData = reinterpret_cast<MetaDataPage *>(parent.ptr);
        auto newRoot = AnyNode::makeRoot(node.pid);
        metaData->root = newRoot.pid;
        parent = std::move(newRoot);
    }
    if (!node->splitNodeWithParent(parent.ptr, key)) {
        auto parentPid = parent.pid;
        parent.release();
        node.release();
        // must split parent first to make space for separator, restart from root to do this
        ensureSpace(parentPid, key);
    }
}

void BTree::ensureSpace(PID innerNode, std::span<uint8_t> key) {
    GuardO<AnyNode> parent(metadataPid);
    GuardO<AnyNode> node(reinterpret_cast<MetaDataPage *>(parent.ptr)->root, parent);
    while (node->isAnyInner() && (node.pid != innerNode)) {
        parent = std::move(node);
        node = GuardO<AnyNode>(parent->lookupInner(key), parent);
    }
    if (node.pid == innerNode) {
        if (node->basic()->freeSpace() >= maxKvSize)
            return; // someone else did split concurrently
        GuardX<AnyNode> parentLocked(std::move(parent));
        GuardX<AnyNode> nodeLocked(std::move(node));
        trySplit(std::move(nodeLocked), std::move(parentLocked), key);
    };
}

bool BTree::lookupImpl(std::span<uint8_t> key, std::span<uint8_t> &valueOut) {
    while (true) {
        try {
            GuardO<AnyNode> parent{metadataPid};
            GuardO<AnyNode> node(reinterpret_cast<MetaDataPage *>(parent.ptr)->root, parent);

            while (node->isAnyInner()) {
                parent = std::move(node);
                node = GuardO<AnyNode>(parent->lookupInner(key), parent);
            }

            while (true) {
                switch (node->tag()) {
                    case Tag::Leaf: {
                        node->basic()->rangeOpCounter.point_op();
                        if (node->basic()->rangeOpCounter.shouldConvertHash()) {
                            GuardX<AnyNode> nodeX(std::move(node));
                            bool converted = nodeX->hash()->tryConvertToBasic();
                            node = std::move(nodeX).downgrade();
                            if (converted)continue;
                        }
                        return node->basic()->lookupLeaf(key, valueOut);
                    }
                    case Tag::Dense:
                    case Tag::Dense2: {
                        return node->dense()->lookup(key, valueOut);
                    }
                    case Tag::Hash: {
                        node->hash()->rangeOpCounter.point_op();
                        return node->hash()->lookup(key, valueOut);
                    }
                    default:
                        ASSUME(false);
                }
                break;
            }
        } catch (const OLCRestartException &) { yield(); }
    }
}

void BTree::range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                             const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
    GuardO<AnyNode> leafGuards[4]{GuardO<AnyNode>::released(), GuardO<AnyNode>::released(), GuardO<AnyNode>::released(),
                                  GuardO<AnyNode>::released()};
    unsigned lockedLeaves = 0;
    std::span<uint8_t> leafKey = key;
    memcpy(keyOutBuffer, key.data(), key.size());
    while (true) {
        GuardO<AnyNode> parent{metadataPid};
        GuardO<AnyNode> node(reinterpret_cast<MetaDataPage *>(parent.ptr)->root, parent);

        while (node->isAnyInner()) {
            parent = std::move(node);
            node = GuardO<AnyNode>(parent->lookupInner(key), parent);
        }
        parent.release();
        if (lockedLeaves >= sizeof(leafGuards) / sizeof(leafGuards[0])) {
            abort();
        }
        while (true) {
            switch (node->tag()) {
                case Tag::Leaf: {
                    node->basic()->rangeOpCounter.range_op();
                    if (!node->basic()->range_lookup(leafKey, keyOutBuffer, found_record_cb))
                        return;
                    key = {keyOutBuffer, node->basic()->upperFence.length};
                    node.checkVersionAndRestart();
                    copySpan(key, node->basic()->getUpperFence());
                    break;
                }
                case Tag::Dense: {
                    node->dense()->range_lookup1(key, keyOutBuffer, found_record_cb);
                    key = {keyOutBuffer, node->dense()->upperFenceLen};
                    node.checkVersionAndRestart();
                    copySpan(key, node->dense()->getUpperFence());
                }
                case Tag::Dense2: {
                    node->dense()->range_lookup2(key, keyOutBuffer, found_record_cb);
                    key = {keyOutBuffer, node->dense()->upperFenceLen};
                    node.checkVersionAndRestart();
                    copySpan(key, node->dense()->getUpperFence());
                }
                case Tag::Hash: {
                    node->hash()->rangeOpCounter.range_op();
                    bool sorted = node->hash()->isSorted();
                    bool convert = node->hash()->rangeOpCounter.shouldConvertBasic();
                    if (!sorted || convert) {
                        GuardX<AnyNode> nodeX(std::move(node));
                        bool converted = false;
                        if (convert && nodeX->hash()->tryConvertToBasic()) {
                            converted = true;
                        }
                        if (!converted) {
                            nodeX->hash()->sort();
                        }
                        node = std::move(nodeX).downgrade();
                        if (converted)
                            continue;
                    }
                    if (!node->hash()->range_lookupImpl(leafKey, keyOutBuffer, found_record_cb))
                        return;
                    key = {keyOutBuffer, node->hash()->upperFenceLen};
                    node.checkVersionAndRestart();
                    copySpan(key, node->hash()->getUpperFence());
                    break;
                }
                default:
                    ASSUME(false);
            }
            break;
        }
        leafKey = {};
        key = {keyOutBuffer, key.size() + 1};
        key[key.size() - 1] = 0;
        leafGuards[lockedLeaves] = std::move(node);
        lockedLeaves += 1;
    }
}

