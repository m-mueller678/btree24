#ifndef BTREE24_ANYNODE_HPP
#define BTREE24_ANYNODE_HPP

#include "Tag.hpp"
#include "BTreeNode.hpp"
#include "HashNode.hpp"
#include "DenseNode.hpp"

union AnyNode {
    TagAndDirty _tag_and_dirty;
    BTreeNode _basic_node;
    DenseNode _dense;
    HashNode _hash;

    Tag tag();

    AnyNode(BTreeNode basic);

    AnyNode();

    void destroy();

    void dealloc();

    static GuardX<AnyNode> allocLeaf();

    static GuardX<AnyNode> allocInner();

    bool isAnyInner();

    BTreeNode *basic();

    DenseNode *dense();

    HashNode *hash();

    bool insertChild(std::span<uint8_t> key, PID child);

    bool innerRequestSpaceFor(unsigned keyLen);

    PID lookupInner(std::span<uint8_t> key);

    static GuardX<AnyNode> makeRoot(PID child);

    void print();

    unsigned lookupInnerIndex(uint8_t *key, unsigned keyLength);

    unsigned innerCount();

    PID getChild(unsigned index);

    void innerRestoreKey(uint8_t *keyOut, unsigned len, unsigned index);

    void innerRemoveSlot(unsigned int slotId);

    unsigned innerKeyLen(unsigned index);

    bool splitNodeWithParent(AnyNode *parent, std::span<uint8_t> key);

    void nodeCount(unsigned counts[TAG_END]);
};


#endif //BTREE24_ANYNODE_HPP
