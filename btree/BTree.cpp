#include "BTree.hpp"
#include "config.hpp"
#include "Tag.hpp"
#include "HashNode.hpp"
#include "BTreeNode.hpp"
#include "DenseNode.hpp"
#include "AnyNode.hpp"


struct MetaDataPage : public TagAndDirty {
    PID root;

    MetaDataPage(PID root) : root(root) {}
};


BTree::~BTree() {
    abort();
}

// take isInt to have same interface as in memory structures, but ignore it.
BTree::BTree(bool isInt) {
    auto root = (enableHash && !enableHashAdapt) ? HashNode::makeRootLeaf() : BTreeNode::makeLeaf();
    auto metadata = GuardX<MetaDataPage>::alloc();
    metadata.pid = root.pid;
    this->metadata_pid = metadata.pid;
#ifndef NDEBUG
    // prevent print from being optimized out. It is otherwise never called, but nice for debugging
    if (getenv("oMEeHAobn4")) {
        root->print();
    }
#endif
}
