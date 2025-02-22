#pragma once

#include <cstdint>
#include <functional>
#include "tuple.hpp"

// The hot headers cause linking errors when included in multiple translation units, so we can not have them here.
// Instead, we declare an opaque struct.
struct Hot;

struct HotBTreeAdapter {
    Hot *hot;

    HotBTreeAdapter(bool isInt);

    ~HotBTreeAdapter();

    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);
};
