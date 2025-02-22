#include "AnyNode.hpp"
#include "common.hpp"

void AnyNode::print() {
    switch (tag()) {
        case Tag::Inner:
        case Tag::Leaf:
            return basic()->print();
        case Tag::Hash:
            return hash()->print();
        case Tag::Dense:
        case Tag::Dense2:
            TODO_UNIMPL //return dense()->print();
    }
}

BTreeNode *AnyNode::basic() {
#ifdef CHECK_TREE_OPS
    Tag t = _tag_and_dirty.tag();
    ASSUME(t == Tag::Leaf || t == Tag::Inner);
#endif
    return reinterpret_cast<BTreeNode *>(this);
}

DenseNode *AnyNode::dense() {
#ifdef CHECK_TREE_OPS
    Tag t = _tag_and_dirty.tag();
    ASSUME(t == Tag::Dense || t == Tag::Dense2);
#endif
    return reinterpret_cast<DenseNode *>(this);
}

HashNode *AnyNode::hash() {
#ifdef CHECK_TREE_OPS
    Tag t = _tag_and_dirty.tag();
    ASSUME(t == Tag::Hash);
#endif
    return reinterpret_cast<HashNode *>(this);
}

Tag AnyNode::tag() {
    Tag t = _tag_and_dirty.tag();
#ifdef CHECK_TREE_OPS
    ASSUME(t == Tag::Inner || t == Tag::Leaf || t == Tag::Dense || t == Tag::Hash || t == Tag::Dense2);
    ASSUME(enableDense || t != Tag::Dense);
    ASSUME((enableDense2 && !enableHash) || t != Tag::Dense2);
    ASSUME(enableHash || t != Tag::Hash);
    ASSUME(!enableHash || enableHashAdapt || t != Tag::Leaf);
#endif
    return t;
}

GuardX<AnyNode> AnyNode::allocLeaf() {
    return GuardX<AnyNode>::alloc();
}

GuardX<AnyNode> AnyNode::allocInner() {
    return GuardX<AnyNode>::alloc();
}

bool AnyNode::isAnyInner() {
    return isInner(tag());
}


PID AnyNode::lookupInner(std::span<uint8_t> key) {
    switch (tag()) {
        case Tag::Inner:
            return basic()->lookupInner(key);
        case Tag::Leaf:
        case Tag::Hash:
        case Tag::Dense:
        case Tag::Dense2:
            ASSUME(false);
    }
    ASSUME(false);
}


bool AnyNode::innerRequestSpaceFor(unsigned keyLen) {
    switch (tag()) {
        case Tag::Inner: {
            return basic()->requestSpaceFor(basic()->spaceNeeded(keyLen, sizeof(PID)));
        }
        case Tag::Leaf:
        case Tag::Hash:
        case Tag::Dense:
        case Tag::Dense2:
            ASSUME(false);
    }
    ASSUME(false);
}

bool AnyNode::splitNodeWithParent(AnyNode *parent, std::span<uint8_t> key) {
    Tag tag = this->tag();
    switch (tag) {
        case Tag::Leaf:
            if (basic()->count <= 2)
                return true;
            if (enableDensifySplit) {
                uint8_t sepBuffer[BTreeNode::maxKVSize];
                auto sep = DenseNode::densifySplit(sepBuffer, basic());
                if (sep.lowerCount != 0) {
                    if (parent->innerRequestSpaceFor(sep.fenceLen)) {
                        bool found;
                        unsigned index = basic()->lowerBound(std::span{sepBuffer, sep.fenceLen}, found);
                        assert(sep.lowerCount == index + found);
                        basic()->splitNode(parent, sep.lowerCount - 1, sepBuffer, sep.fenceLen);
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            // continue with normal node split
        case Tag::Inner: {
            if (basic()->count <= 2)
                return true;
            SeparatorInfo sepInfo = basic()->findSeparator();
            if (parent->innerRequestSpaceFor(
                    sepInfo.length)) {  // is there enough space in the parent for the separator?
                uint8_t sepKey[sepInfo.length];
                assert(basic()->count > 1);
                basic()->getSep(sepKey, sepInfo);
                basic()->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
                return true;
            } else {
                return false;
            }
        }
        case Tag::Dense2:
        case Tag::Dense: {
            if (dense()->slotCount <= 2)
                return true;
            if (parent->innerRequestSpaceFor(
                    dense()->fullKeyLen)) {  // is there enough space in the parent for the separator?
                if (tag == Tag::Dense)
                    dense()->splitNode1(parent, key);
                else
                    dense()->splitNode2(parent, key);
                return true;
            } else {
                return false;
            }
            break;
        }
        case Tag::Hash: {
            if (hash()->count <= 2)
                return true;
            hash()->sort();
            SeparatorInfo sepInfo = hash()->findSeparator();
            if (parent->innerRequestSpaceFor(
                    sepInfo.length)) {  // is there enough space in the parent for the separator?
                uint8_t sepKey[sepInfo.length];
                hash()->getSep(sepKey, sepInfo);
                hash()->splitNode(parent, sepInfo.slot, {sepKey, sepInfo.length});
                return true;
            } else {
                return false;
            }
            break;
        }
    }
    ASSUME(false);
}


GuardX<AnyNode> AnyNode::makeRoot(PID child) {
    auto new_root = allocInner();
    new_root->_basic_node.init(false, RangeOpCounter{});
    new_root->basic()->upper = child;
    return new_root;
}

AnyNode::AnyNode() {}

bool AnyNode::insertChild(std::span<uint8_t> key, PID child) {
    switch (tag()) {
        case Tag::Inner:
            return basic()->insertChild(key, child);
        case Tag::Leaf:
        case Tag::Hash:
        case Tag::Dense:
        case Tag::Dense2:
            ASSUME(false);
    }
    ASSUME(false);
}

void HashNode::print() {
    printf("# HashNode\n");
    printf("lower fence: ");
    for (unsigned i = 0; i < lowerFenceLen; ++i) {
        printf("%d, ", getLowerFence()[i]);
    }
    printf("\nupper fence: ");
    for (unsigned i = 0; i < upperFenceLen; ++i) {
        printf("%d, ", getUpperFence()[i]);
    }
    printf("\n");
    for (unsigned i = 0; i < count; ++i) {
        printf("%4d: [%3d] ", i, hashes()[i]);
        printKey(getKey(i));
        printf("\n");
    }
}
