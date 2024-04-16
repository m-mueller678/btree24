#ifndef BTREE24_BTREE_HPP
#define BTREE24_BTREE_HPP

#include <cstdint>
#include <functional>
#include "vmache.hpp"
#include "nodes.hpp"

struct BTree {
    BTree(bool isInt);

    ~BTree();

    PID metadataPid;

    bool lookupImpl(std::span<uint8_t> key, std::span<uint8_t> &valueOut);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);

    void trySplit(GuardX<AnyNode> node, GuardX<AnyNode> parent, std::span<uint8_t> key);

    void ensureSpace(PID innerNode, std::span<uint8_t> key);
};


#endif //BTREE24_BTREE_HPP
