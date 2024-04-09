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

            GuardX<AnyNode> nodeLocked{std::move(node)};
            switch (nodeLocked->tag()) {
                case Tag::Leaf: {
                    nodeLocked->basic()->rangeOpCounter.point_op();
                    if (nodeLocked->basic()->rangeOpCounter.shouldConvertHash()) { TODO_UNIMPL }
                    if (nodeLocked->basic()->insert(key, payload)) {
                        parent.release_ignore();
                        return;
                    }
                }
                case Tag::Dense:
                case Tag::Dense2: {
                    TODO_UNIMPL
                }
                case Tag::Hash: {
                    TODO_UNIMPL
                }
                default:
                    ASSUME(false);
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
        parent.release();
        node.release();
        // must split parent first to make space for separator, restart from root to do this
        ensureSpace(parent.pid, key);
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

            switch (node->tag()) {
                case Tag::Leaf: {
                    node->basic()->rangeOpCounter.point_op();
                    if (node->basic()->rangeOpCounter.shouldConvertHash()) { TODO_UNIMPL }
                    return node->basic()->lookupLeaf(key, valueOut);
                }
                case Tag::Dense:
                case Tag::Dense2: {
                    TODO_UNIMPL
                }
                case Tag::Hash: {
                    TODO_UNIMPL
                }
                default:
                    ASSUME(false);
            }
        } catch (const OLCRestartException &) { yield(); }
    }
}
