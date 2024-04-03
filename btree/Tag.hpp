#ifndef BTREE24_TAG_HPP
#define BTREE24_TAG_HPP

#include<atomic>
#include <random>
#include "config.hpp"

enum class Tag : uint8_t {
    Inner = 1,
    Leaf = 2,
    Dense = 3,
    Hash = 4,
    Dense2 = 5,
    _last = 5,
};

constexpr unsigned TAG_END = unsigned(Tag::_last) + 1;

const char *tag_name(Tag tag);

struct RangeOpCounter {
    std::atomic<uint8_t> count;
    static constexpr uint8_t MAX_COUNT = 3;

    thread_local static std::bernoulli_distribution range_dist;
    thread_local static std::bernoulli_distribution point_dist;
    thread_local static std::minstd_rand rng;

    void init(uint8_t c = MAX_COUNT / 2) {
        count.store(c, std::memory_order_relaxed);
    }

    void setGoodHeads() {
        count = 255;
    }

    void setBadHeads(uint8_t previous = MAX_COUNT / 2) {
        if (previous == 255) {
            count = MAX_COUNT / 2;
        } else {
            count = previous;
        }
    }

    static constexpr uint32_t RANGE_THRESHOLD = (std::minstd_rand::max() + 1) * 0.15;
    static constexpr uint32_t POINT_THRESHOLD = (std::minstd_rand::max() + 1) * 0.05;

    void range_op() {
        bool should_inc = false;
        while (true) {
            auto c = count.load(std::memory_order_relaxed);
            if (count < MAX_COUNT) {
                if (!should_inc) {
                    should_inc = rng() < RANGE_THRESHOLD;
                }
                if (!should_inc) { break; }
                if (should_inc) {
                    if (count.compare_exchange_weak(c, c + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    } else {
                        continue;
                    }
                }
            } else {
                break;
            }
        }


    }

    void point_op() {
        bool should_dec = false;
        while (true) {
            auto c = count.load(std::memory_order_relaxed);
            if (std::int8_t(count) > 0) {
                if (!should_dec) {
                    should_dec = rng() < POINT_THRESHOLD;
                }
                if (!should_dec) { break; }
                if (should_dec) {
                    if (count.compare_exchange_weak(c, c + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    } else {
                        continue;
                    }
                }
            } else {
                break;
            }
        }
    }

    bool isLowRange() {
        return count.load(std::memory_order_relaxed) <= MAX_COUNT / 2;
    }
};


struct TagAndDirty {
private:
    uint8_t x;
public:
    RangeOpCounter rangeOpCounter;

    void init(Tag t, RangeOpCounter roc) {
        x = 128 | static_cast<uint8_t>(t);
        rangeOpCounter.init(roc.count.load(std::memory_order_relaxed));
    }

    Tag tag() { return static_cast<Tag>(x & 127); }

    void set_tag(Tag t) { x = static_cast<uint8_t>(t) | (x & 128); }

    void set_dirty(bool d) { x = x & 127 | static_cast<uint8_t>(d) << 7; }

    bool dirty() { return (x & 128) != 0; }
};


#endif //BTREE24_TAG_HPP
