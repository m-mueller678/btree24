#include "wh_adapter.hpp"
#include "vendor/wormhole/lib.h"
#include "vendor/wormhole/kv.h"
#include "vendor/wormhole/wh.h"
#include "tuple.hpp"
#include "btree/config.hpp"

// TODO this uses the wh api, using the wormhole api might be more efficient, but needs more care about concurrency.
//  https://github.com/wuxb45/wormhole

struct kv *
kvmap_mm_out_noop(struct kv *const kv, struct kv *const out) {
    return kv;
}

const struct kvmap_mm kvmap_mm_btree = {
        .in = kvmap_mm_in_noop,
        .out = kvmap_mm_out_noop,
        .free = kvmap_mm_free_free,
        .priv = NULL,
};

WhBTreeAdapter::WhBTreeAdapter(bool isInt) {
    wh = wh_create();
}

void WhBTreeAdapter::lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback) {
    struct wormref *ref = wh_ref(wh);
    u8 buf[maxKvSize];
    u32 len_out;
    if (wh_get(ref, key.data(), key.size(), buf, maxKvSize, &len_out)) {
        callback({buf, len_out});
    }
    wh_unref(ref);
}

void WhBTreeAdapter::insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload) {
    struct wormref *ref = wh_ref(wh);
    wh_put(ref, key.data(), key.size(), payload.data(), payload.size());
    wh_unref(ref);
}

void WhBTreeAdapter::range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                                      const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
    struct wormref *ref = wh_ref(wh);

    struct wormhole_iter *iter = wh_iter_create(ref);
    wh_iter_seek(iter, key.data(), key.size());
    uint32_t keyLenOut;
    uint8_t bufferOut[maxKvSize];
    uint32_t valLenOut;
    while (wh_iter_peek(iter, keyOutBuffer, maxKvSize, &keyLenOut, bufferOut, maxKvSize, &valLenOut)) {
        if (!found_record_cb(keyLenOut, {bufferOut, valLenOut}))
            break;
        wh_iter_skip1(iter);
    }
    wh_iter_destroy(iter);
    wh_unref(ref);
}



