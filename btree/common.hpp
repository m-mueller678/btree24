#ifndef BTREE24_COMMON_HPP
#define BTREE24_COMMON_HPP

#include <cstdint>
#include <span>

#define ASSUME(x) do{assert(x); if (!(x)) __builtin_unreachable();}while(0)

#define TODO_UNIMPL abort();

void printKey(std::span<uint8_t> key);

template<class T>
static T loadUnaligned(void *p) {
    T x;
    memcpy(&x, p, sizeof(T));
    return x;
}

template<class T>
static void storeUnaligned(void *p, T t) {
    memcpy(p, &t, sizeof(T));
}


#endif //BTREE24_COMMON_HPP
