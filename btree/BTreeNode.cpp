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
    return slice(upperFence.offset, upperFence.length);
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

PID BTreeNode::getChild(unsigned slotId) {
    assert(isInner());
    return loadUnaligned<PID>(getPayload(slotId).data());
}

void BTreeNode::init(bool isLeaf, RangeOpCounter roc) {
    BTreeNodeHeader::init(isLeaf, roc);
}

void BTreeNodeHeader::init(bool isLeaf, RangeOpCounter roc) {
    set_tag(isLeaf ? Tag::Leaf : Tag::Inner);
    rangeOpCounter = roc;
}

uint8_t *BTreeNode::ptr() {
    return reinterpret_cast<uint8_t *>(this);
}