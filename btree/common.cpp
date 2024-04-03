#include <cstdio>
#include "common.hpp"

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
