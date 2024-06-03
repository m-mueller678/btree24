#pragma once

#include <span>
#include <cstdint>
#include <c++/11/functional>

struct wormhole;

struct WhAdapter {
    wormhole *wh;

    WhAdapter(bool isInt);

    ~WhAdapter();

    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);
};