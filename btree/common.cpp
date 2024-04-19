#include <cstdio>
#include "common.hpp"
#include "config.hpp"
#include "vmache.hpp"

void printKey(std::span<uint8_t> key) {
    if (key.size() <= 4) {
        for (unsigned i = 0; i < key.size(); ++i) {
            printf("%3d, ", key[i]);
        }
        printf("|  ");
    }
    for (unsigned i = 0; i < key.size(); ++i) {
        if (key[i] >= ' ' && key[i] <= '~') {
            putchar(key[i]);
        } else {
            printf("\\%02x", key[i]);
        }
    }
}

std::vector<uint8_t> toByteVector(std::span<uint8_t> k) {
    return std::vector(k.begin(), k.end());
}

uint32_t head(std::span<uint8_t> key) {
    switch (key.size()) {
        case 0:
            return 0;
        case 1:
            return static_cast<uint32_t>(key[0]) << 24;
        case 2:
            return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key.data()))) << 16;
        case 3:
            return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key.data()))) << 16) |
                   (static_cast<uint32_t>(key[2]) << 8);
        default:
            return __builtin_bswap32(loadUnaligned<uint32_t>(key.data()));
    }
}

std::strong_ordering span_compare(std::span<uint8_t> a, std::span<uint8_t> b) {
    return std::lexicographical_compare_three_way(a.begin(), a.end(), b.begin(), b.end());
}

unsigned commonPrefixLength(std::span<uint8_t> a, std::span<uint8_t> b) {
    unsigned limit = min(a.size(), b.size());
    unsigned i;
    for (i = 0; i < limit; i++)
        if (a[i] != b[i])
            break;
    return i;
}

std::span<uint8_t> optimistic_memcpy(uint8_t *buffer, uint32_t offset, std::span<uint8_t> x) {
    if (offset + x.size() > maxKvSize)
        throw OLCRestartException();
    memcpy(buffer + offset, x.data(), x.size());
    return {buffer, offset + x.size()};
}
