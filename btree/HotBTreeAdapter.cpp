#include "HotBTreeAdapter.hpp"
#include "hot/rowex/HOTRowex.hpp"
#include "tuple.hpp"

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

namespace idx {
    namespace contenthelpers {
        template<>
        inline auto toFixSizedKey(TupleKeyRef const &key) {
            constexpr size_t maxLen = getMaxKeyLength<char const *>();
            std::array<uint8_t, maxLen> fixedSizeKey;
            assert(key.length <= maxLen);
            memcpy(fixedSizeKey.data(), key.data, key.length);
            memset(fixedSizeKey.data() + key.length, 0, maxLen - key.length);
            return fixedSizeKey;
        }

        template<>
        constexpr inline size_t getMaxKeyLength<TupleKeyRef>() {
            return getMaxKeyLength<char const *>();
        }
    }  // namespace contenthelpers
}  // namespace idx

void HotBTreeAdapter::lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback) {
    if (hot->isInt) {
        auto it = hot->int_hot.find(*reinterpret_cast<std::array<uint8_t, 4> *>(key.data()));
        if (it == hot->int_hot.end())
            return;
        Tuple *tuple = *it;
        callback(tuple->payload());
    } else {
        auto it = hot->string_hot.find(TupleKeyRef{key.data(), key.size()});
        if (it == hot->string_hot.end())
            return;
        Tuple *tuple = *it;
        callback(tuple->payload());
    }
}

void HotBTreeAdapter::insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload) {
    // if the key already exists, we leak the old tuple because cleanup is hard and the benchmark does not do updates anyway.
    if (hot->isInt) {
        uintptr_t tuple = Tuple::makeTuple(key, payload);
        hot->int_hot.upsert(reinterpret_cast<Tuple *>(tuple));
    } else {
        uintptr_t tuple = Tuple::makeTuple(key, payload);
        hot->string_hot.upsert(reinterpret_cast<Tuple *>(tuple));
    }
}

void HotBTreeAdapter::range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                                       const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
    if (hot->isInt) {
        auto it = hot->int_hot.lower_bound(*reinterpret_cast<std::array<uint8_t, 4> *>(key.data()));
        while (true) {
            if (it == hot->int_hot.end())
                break;
            Tuple *tuple = *it;
            memcpy(keyOutBuffer, tuple->data, tuple->keyLen);
            if (!found_record_cb(tuple->keyLen, tuple->payload())) {
                break;
            }
            ++it;
        }
    } else {
        auto it = hot->string_hot.lower_bound(TupleKeyRef{key.data(), key.size()});
        while (true) {
            if (it == hot->string_hot.end())
                break;
            Tuple *tuple = *it;
            memcpy(keyOutBuffer, tuple->data, tuple->keyLen);
            if (!found_record_cb(tuple->keyLen, tuple->payload())) {
                break;
            }
            ++it;
        }
    }
}

HotBTreeAdapter::HotBTreeAdapter(bool isInt) : hot(new Hot{HotSS{}, HotSSI{}, isInt}) {}

HotBTreeAdapter::~HotBTreeAdapter() {
    delete hot;
}