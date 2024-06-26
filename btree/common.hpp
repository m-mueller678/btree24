#ifndef BTREE24_COMMON_HPP
#define BTREE24_COMMON_HPP

#include <cstdint>
#include <span>
#include <vector>
#include <cassert>
#include <cstring>
#include <mutex>
#include <atomic>

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

std::vector<uint8_t> toByteVector(std::span<uint8_t> k);

template<class T>
void destroy(T &&x) {
    T a = x;
};

template<class T>
void copySpan(std::span<T> dst, std::span<T> src) {
    assert(dst.size() == src.size());
    std::copy(src.begin(), src.end(), dst.begin());
}

// Get order-preserving head of key (assuming little endian)
uint32_t head(std::span<uint8_t> key);

inline unsigned min(unsigned a, unsigned b) {
    return a < b ? a : b;
}

inline unsigned max(unsigned a, unsigned b) {
    return a < b ? b : a;
}

std::strong_ordering span_compare(std::span<uint8_t> a, std::span<uint8_t> b);


unsigned commonPrefixLength(std::span<uint8_t> a, std::span<uint8_t> b);

std::span<uint8_t> optimistic_memcpy(uint8_t *buffer, uint32_t offset, std::span<uint8_t>);

static std::mutex print_mutex;

#define COUNTER(label, increment, interval) \
do{                                         \
    static std::atomic<uint64_t > label ## _count =0; \
    static std::atomic<uint64_t > label ## _sum =0;   \
    uint64_t count = 1+(label ## _count).fetch_add(1,std::memory_order_relaxed); \
    uint64_t sum = increment+(label ## _sum).fetch_add(increment,std::memory_order_relaxed); \
    if(count % ((uint64_t)(1)<<interval) == 0 ){\
        print_mutex.lock();                   \
        std::cout<<"counter "<<#label<<" "<<sum<<"/"<<count<<"="<<double(sum)/count<<std::endl; \
        print_mutex.unlock();\
    }\
}while(0);
#endif //BTREE24_COMMON_HPP

inline uint64_t envOr(const char *env, uint64_t value) {
    if (getenv(env))
        return atof(getenv(env));
    return value;
}