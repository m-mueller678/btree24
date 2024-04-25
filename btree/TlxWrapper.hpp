
#ifndef BTREE24_TLXWRAPPER_HPP
#define BTREE24_TLXWRAPPER_HPP

#include <cstdint>
#include <functional>
#include <span>

struct TlxImpl;

struct TlxWrapper {
    TlxImpl *impl;

    TlxWrapper(bool isInt);

    ~TlxWrapper();

    void lookupImpl(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback);

    void insertImpl(std::span<uint8_t> key, std::span<uint8_t> payload);

    void range_lookupImpl(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                          const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb);
};


#endif //BTREE24_TLXWRAPPER_HPP
