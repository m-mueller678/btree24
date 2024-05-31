#pragma once

#include <stdlib.h>
#include <cstring>
#include <cstdint>
#include <span>

constexpr bool ZERO_TERMINATE = false;

struct Tuple {
    uint16_t keyLen;
    uint16_t payloadLen;
    uint8_t data[];

    uint8_t *payload() { return data + keyLen + ZERO_TERMINATE; }

    static uintptr_t makeTuple(std::span<uint8_t> key, std::span<uint8_t> payload) {
        Tuple *tuple = reinterpret_cast<Tuple *>(malloc(sizeof(Tuple) + key.size() + ZERO_TERMINATE + payload.size()));
        tuple->keyLen = key.size();
        tuple->payloadLen = payload.size();
        memcpy(tuple->data, key.data(), key.size());
        if (ZERO_TERMINATE)
            tuple->data[key.size()] = 0;
        memcpy(tuple->payload(), payload.data(), payload.size());
        return reinterpret_cast<uintptr_t>(tuple);
    }

    static uint8_t *tuplePayloadPtr(uintptr_t tuple) { return reinterpret_cast<Tuple *>(tuple)->payload(); }

    static uint8_t *tupleKeyPtr(uintptr_t tuple) { return reinterpret_cast<Tuple *>(tuple)->data; }

    static unsigned tuplePayloadLen(uintptr_t tuple) { return reinterpret_cast<Tuple *>(tuple)->payloadLen; }

    static unsigned tupleKeyLen(uintptr_t tuple) { return reinterpret_cast<Tuple *>(tuple)->keyLen; }
};
