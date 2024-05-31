#pragma once

#include "vmache.hpp"
#include "tuple.hpp"

#define _mm_set_pi64x (__m64)

#include "hot/rowex/HOTRowex.hpp"

#undef _mm_set_pi64x

struct TupleKeyRef {
    uint8_t *data;
    unsigned length;

    bool operator==(const TupleKeyRef &rhs) const {
        return length == rhs.length && memcmp(data, rhs.data, length) == 0;
    }

    bool operator<(const TupleKeyRef &rhs) const {
        return std::basic_string_view{data, length} < std::basic_string_view{rhs.data, rhs.length};
    }
};

template<typename T>
struct HotTupleKeyExtractor {
    template<typename K>
    TupleKeyRef operator()(K);
};

template<>
template<>
TupleKeyRef HotTupleKeyExtractor<Tuple *>::operator()(Tuple *t) {
    return TupleKeyRef{t->data, t->keyLen};
};

template<>
template<>
TupleKeyRef HotTupleKeyExtractor<Tuple *>::operator()(const TupleKeyRef &k) {
    return k;
};

template<>
template<>
TupleKeyRef HotTupleKeyExtractor<Tuple *>::operator()(TupleKeyRef k) {
    return k;
};

template<typename T>
struct HotTupleIntKeyExtractor {
    template<typename K>
    std::array<uint8_t, 4> operator()(K);
};

template<>
template<>
std::array<uint8_t, 4> HotTupleIntKeyExtractor<Tuple *>::operator()(Tuple *t) {
    assert(t->keyLen == 4);
    std::array<uint8_t, 4> x;
    memcpy(&x, t->data, 4);
    return x;
};

template<>
template<>
std::array<uint8_t, 4> HotTupleIntKeyExtractor<Tuple *>::operator()(std::array<uint8_t, 4> x) {
    return x;
};

typedef hot::rowex::HOTRowex<Tuple *, HotTupleKeyExtractor> HotSS;
typedef hot::rowex::HOTRowex<Tuple *, HotTupleIntKeyExtractor> HotSSI;

struct Hot {
    HotSS string_hot;
    HotSSI int_hot;
    bool isInt;
};

struct HotWrapper {
    HotWrapper(bool isInt);

    ~HotWrapper();

    PID metadataPid;

    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);
};