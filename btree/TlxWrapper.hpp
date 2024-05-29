#pragma once

#include "config.hpp"

#ifdef USE_STRUCTURE_TLX

#include <algorithm>
#include <cstdint>
#include <functional>
#include "vmache.hpp"
#include "nodes.hpp"
#include "container/btree_map.hpp"

struct KeyType {
    std::span<uint8_t> data;
    bool owned;

    KeyType();

    KeyType(std::span<uint8_t> data) : data(data), owned(false) {}

    KeyType(const KeyType &other);

    KeyType(KeyType &&other);

    ~KeyType();

    void makeOwned();

    bool operator<(const KeyType &other) const;

    bool operator<=(const KeyType &other) const;

    KeyType &operator=(const KeyType &other);

    KeyType &operator=(KeyType &&other);
};

struct TlxWrapper {
    TlxWrapper(bool isInt);

    ~TlxWrapper();

    bool isInt;
    tlx::btree_map<uint32_t, std::vector<uint8_t>, std::less<uint32_t>, tlx::btree_default_traits<uint32_t, std::vector<uint8_t>, BTREE_CMAKE_PAGE_SIZE, BTREE_CMAKE_PAGE_SIZE>,
            std::allocator<std::pair<uint32_t, std::vector<uint8_t>>>, true> integers;
    tlx::btree_map<KeyType, std::vector<uint8_t>, std::less<KeyType>, tlx::btree_default_traits<KeyType, std::vector<uint8_t>, BTREE_CMAKE_PAGE_SIZE, BTREE_CMAKE_PAGE_SIZE>,
            std::allocator<std::pair<KeyType, std::vector<uint8_t>>>, true> strings;

    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);
};

#endif