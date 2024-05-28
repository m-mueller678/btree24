#pragma once

#include "config.hpp"

#ifdef USE_STRUCTURE_TLX

#include <cstdint>
#include <functional>
#include "vmache.hpp"
#include "nodes.hpp"
#include "container/btree_map.hpp"

struct TlxWrapper {
    TlxWrapper(bool isInt);

    ~TlxWrapper();

    tlx::btree_map<uint32_t, std::vector<uint8_t>, std::less<uint32_t>, tlx::btree_default_traits<uint32_t, uint32_t, BTREE_CMAKE_PAGE_SIZE, BTREE_CMAKE_PAGE_SIZE>,
            std::allocator<uint32_t>, true> integers;

    tlx::btree_map<std::vector<uint8_t>, std::vector<uint8_t>, std::less<std::vector<uint8_t>>, tlx::btree_default_traits<std::vector<uint8_t>, std::vector<uint8_t>, BTREE_CMAKE_PAGE_SIZE, BTREE_CMAKE_PAGE_SIZE>,
            std::allocator<std::vector<uint8_t>>, true> strings;

    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);

    void trySplit(GuardX<AnyNode> node, GuardX<AnyNode> parent, std::span<uint8_t> key);

    void ensureSpace(PID innerNode, std::span<uint8_t> key);

    void nodeCount(std::array<uint32_t, TAG_END + 1> &counts);
};

#endif