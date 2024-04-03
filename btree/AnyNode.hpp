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

    bool insertChild(uint8_t *key, unsigned int keyLength, AnyNode *child);

    bool innerRequestSpaceFor(unsigned keyLen);

    PID lookupInner(uint8_t *key, unsigned keyLength);

    static GuardX<AnyNode> makeRoot(PID child);

    void print();

    unsigned lookupInnerIndex(uint8_t *key, unsigned keyLength);

    unsigned innerCount();

    PID getChild(unsigned index);

    void innerRestoreKey(uint8_t *keyOut, unsigned len, unsigned index);

    void innerRemoveSlot(unsigned int slotId);

    unsigned innerKeyLen(unsigned index);

    bool splitNodeWithParent(AnyNode *parent, uint8_t *key, unsigned keyLength);

    void nodeCount(unsigned counts[TAG_END]);
};


#endif //BTREE24_ANYNODE_HPP
