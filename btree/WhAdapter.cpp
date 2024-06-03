
#include "WhAdapter.hpp"
#include "wormhole/lib.h"
#include "wormhole/ctypes.h"
#include "wormhole/kv.h"
#include "wormhole/wh.h"
#include "config.hpp"

WhAdapter::WhAdapter(bool isInt) {
    wh = wh_create();
}

WhAdapter::~WhAdapter() {
    wh_destroy(wh);
}

void WhAdapter::lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback) {
    struct wormref *ref = wh_ref(wh);
    u8 buffer[maxKvSize];
    u32 valLenOut;
    if (wh_get(ref, key.data(), key.size(), buffer, BTREE_CMAKE_PAGE_SIZE, &valLenOut)) {
        callback({buffer, valLenOut});
    }
    wh_unref(ref);
}

void WhAdapter::insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload) {
    struct wormref *ref = wh_ref(wh);
    wh_put(ref, key.data(), key.size(), payload.data(), payload.size());
    wh_unref(ref);
}

void WhAdapter::range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                                 const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
    struct wormref *ref = wh_ref(wh);
    struct wormhole_iter *iter = wh_iter_create(ref);
    u8 valOut[maxKvSize];
    u32 keyLenOut;
    u32 valLenOut;
    wh_iter_seek(iter, key.data(), key.size());
    while (true) {
        if (!wh_iter_valid(iter))
            break;
        wh_iter_peek(iter, keyOutBuffer, maxKvSize, &keyLenOut, valOut, maxKvSize, &valLenOut);
        if (!found_record_cb(keyLenOut, {valOut, valLenOut}))
            break;
        wh_iter_skip1(iter);
    }
    wh_iter_destroy(iter);
    wh_unref(ref);
}

