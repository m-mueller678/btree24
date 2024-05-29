#include "config.hpp"
#ifdef USE_STRUCTURE_TLX

#include "TlxWrapper.hpp"
#include "common.hpp"


uint32_t unwrap_int_key(std::span<uint8_t> key) {
    if (key.size() != 4)
        throw;
    return __builtin_bswap32(loadUnaligned<uint32_t>(key.data()));
}

TlxWrapper::TlxWrapper(bool isInt) : isInt(false), integers(), strings() {}

TlxWrapper::~TlxWrapper() {}

void TlxWrapper::lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback) {
    if (isInt) {
        auto it = integers.find(unwrap_int_key(key));
        if (it != integers.end()) {
            callback(it->second);
        }
    } else {
        auto it = strings.find(KeyType{key});
        if (it != strings.end()) {
            callback(it->second);
        }
    }
}

void TlxWrapper::insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload) {
    std::vector<uint8_t> p{payload.begin(), payload.end()};
    if (isInt) {
        integers.insert(std::make_pair(unwrap_int_key(key), p));
    } else {
        KeyType k{key};
        k.makeOwned();
        strings.insert(std::make_pair(std::move(k), p));
    }
}

KeyType::KeyType(const KeyType &other) : data(other.data), owned(other.owned) {
    if (owned) {
        owned = false;
        makeOwned();
    }
}

void KeyType::makeOwned() {
    if (owned)
        abort();
    auto ptr = new uint8_t[data.size()];
    memcpy(ptr, data.data(), data.size());
    data = {ptr, data.size()};
}

bool KeyType::operator<(const KeyType &other) const {
    return std::lexicographical_compare(data.begin(), data.end(), other.data.begin(), other.data.end());
}

bool KeyType::operator<=(const KeyType &other) const {
    return !(other < *this);
}

KeyType::~KeyType() {
    if (owned)
        delete[] data.data();
}

KeyType::KeyType(KeyType &&other) : data(other.data), owned(other.owned) {
    other.owned = false;
}

KeyType &KeyType::operator=(const KeyType &other) {
    if (owned)
        delete[] data.data();
    data = other.data;
    owned = false;
    if (other.owned)
        makeOwned();
    return *this;
}

KeyType &KeyType::operator=(KeyType &&other) {
    if (owned)
        delete[] data.data();
    data = other.data;
    owned = other.owned;
    other.owned = false;
    return *this;
}

static uint8_t EMPTY_KEY = 0;

KeyType::KeyType() : data({&EMPTY_KEY, 0}), owned(false) {}

void TlxWrapper::range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                                  const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
    if (isInt) {
        auto it = integers.find(unwrap_int_key(key));
        while (it != integers.end()) {
            union {
                uint32_t x;
                uint8_t b[4];
            };
            x = __builtin_bswap32(it->first);
            memcpy(keyOutBuffer, b, 4);
            if (!found_record_cb(4, it->second))
                return;
            ++it;
        }
    } else {
        auto it = strings.find(KeyType{key});
        while (it != strings.end()) {
            memcpy(keyOutBuffer, it->first.data.data(), it->first.data.size());
            if (!found_record_cb(it->first.data.size(), it->second))
                return;
            ++it;
        }
    }
}

#endif
