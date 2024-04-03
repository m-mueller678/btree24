#include "BTree.hpp"
#include "config.hpp"


BTree::~BTree() {
    abort();
}

BTree::BTree(bool isInt) {
    abort();
//    auto root = (enableHash && !enableHashAdapt) ? HashNode::makeRootLeaf() : BTreeNode::makeLeaf();
//    metadata_pid = AllocGuard<MetaDataPage>{root.pid}.pid;
//#ifndef NDEBUG
//    // prevent print from being optimized out. It is otherwise never called, but nice for debugging
//    if (getenv("oMEeHAobn4")){
//        root->print();
//    }
//#endif
}