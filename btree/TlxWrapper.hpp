#pragma once

#include "config.hpp"

#ifdef USE_STRUCTURE_TLX

#include <algorithm>
#include <cstdint>
#include <functional>
#include "vmache.hpp"
#include "nodes.hpp"
#include "container/btree_map.hpp"

struct TlxKey {
    std::span<uint8_t> data;
    bool owned;

    TlxKey();

    TlxKey(std::span<uint8_t> data) : data(data), owned(false) {}

    TlxKey(const TlxKey &other);

    TlxKey(TlxKey &&other);

    ~TlxKey();

    void makeOwned();

    bool operator==(const TlxKey &other) const;

    bool operator==(const int &other) const;

    bool operator<(const TlxKey &other) const;

    bool operator>(const TlxKey &other) const;

    bool operator>=(const TlxKey &other) const;

    bool operator<=(const TlxKey &other) const;

    TlxKey &operator=(const TlxKey &other);

    TlxKey &operator=(TlxKey &&other);
};

struct TlxWrapper {
    TlxWrapper(bool isInt);

    ~TlxWrapper();

    bool isInt;
    tlx::btree_map<uint32_t, std::vector<uint8_t>, std::less<uint32_t>, tlx::btree_default_traits<uint32_t, std::vector<uint8_t>, BTREE_CMAKE_PAGE_SIZE, BTREE_CMAKE_PAGE_SIZE>,
            std::allocator<std::pair<uint32_t, std::vector<uint8_t>>>, true> integers;
    tlx::btree_map<TlxKey, std::vector<uint8_t>, std::less<TlxKey>, tlx::btree_default_traits<TlxKey, std::vector<uint8_t>, BTREE_CMAKE_PAGE_SIZE, BTREE_CMAKE_PAGE_SIZE>,
            std::allocator<std::pair<TlxKey, std::vector<uint8_t>>>, true> strings;

    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);
};

#endif