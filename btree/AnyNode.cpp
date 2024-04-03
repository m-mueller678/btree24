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
