#include "AnyNode.hpp"
#include "common.hpp"

void AnyNode::print() {
    switch (tag()) {
        case Tag::Inner:
        case Tag::Leaf:
            return basic()->print();
        case Tag::Hash:
            TODO_UNIMPL //return hash()->print();
        case Tag::Dense:
        case Tag::Dense2:
            TODO_UNIMPL //return dense()->print();
    }
}

BTreeNode *AnyNode::basic() {
    Tag t = _tag_and_dirty.tag();
    ASSUME(t == Tag::Leaf || t == Tag::Inner);
    return reinterpret_cast<BTreeNode *>(this);
}

DenseNode *AnyNode::dense() {
    Tag t = _tag_and_dirty.tag();
    ASSUME(t == Tag::Dense || t == Tag::Dense2);
    return reinterpret_cast<DenseNode *>(this);
}

HashNode *AnyNode::hash() {
    Tag t = _tag_and_dirty.tag();
    ASSUME(t == Tag::Hash);
    return reinterpret_cast<HashNode *>(this);
}

Tag AnyNode::tag() {
    Tag t = _tag_and_dirty.tag();
    ASSUME(t == Tag::Inner || t == Tag::Leaf || t == Tag::Dense || t == Tag::Hash || t == Tag::Dense2);
    ASSUME(enableDense || t != Tag::Dense);
    ASSUME((enableDense2 && !enableHash) || t != Tag::Dense2);
    ASSUME(enableHash || t != Tag::Hash);
    ASSUME(!enableHash || enableHashAdapt || t != Tag::Leaf);
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
            if (enableDensifySplit) {
                uint8_t sepBuffer[BTreeNode::maxKVSize];
                TODO_UNIMPL
//                auto sep = DenseNode::densifySplit(sepBuffer, basic());
//                if (sep.lowerCount != 0) {
//                    if (parent->innerRequestSpaceFor(sep.fenceLen)) {
//                        bool found;
//                        unsigned index = basic()->lowerBound(std::span{sepBuffer, sep.fenceLen}, found);
//                        assert(sep.lowerCount == index + found);
//                        basic()->splitNode(parent, sep.lowerCount - 1, sepBuffer, sep.fenceLen);
//                        return true;
//                    } else {
//                        return false;
//                    }
//                }
            }
            // continue with normal node split
        case Tag::Inner: {
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
        case Tag::Dense:
        case Tag::Dense2: {
            TODO_UNIMPL
        }
        case Tag::Hash: {
            TODO_UNIMPL
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

