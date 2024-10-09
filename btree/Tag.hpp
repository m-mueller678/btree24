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

bool isInner(Tag t);

constexpr unsigned TAG_START = unsigned(Tag::Inner);
constexpr unsigned TAG_END = unsigned(Tag::_last) + 1;

const char *tag_name(Tag tag);

struct ContentionSplitException {
};

struct RangeOpCounter {
    std::atomic<uint8_t> count;
    static constexpr uint8_t MAX_COUNT = 3;

    RangeOpCounter() { init(); }

    RangeOpCounter(RangeOpCounter const &other) : count(other.count.load(std::memory_order::relaxed)) {}

    RangeOpCounter &operator=(RangeOpCounter const &other) {
        count = other.count.load(std::memory_order::relaxed);
        return *this;
    }

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

    void range_op();

    void point_op();

    bool isLowRange() {
        return count.load(std::memory_order_relaxed) <= MAX_COUNT / 2;
    }

    bool shouldConvertHash() {
        return enableHashAdapt && count == 0;
    }

    bool shouldConvertBasic() {
        return enableHashAdapt && count == MAX_COUNT;
    }
};

#ifdef USE_STRUCTURE_BTREE
struct TagAndDirty {
private:
    uint8_t x;
public:
    RangeOpCounter rangeOpCounter;
private:
    std::uint16_t contentionCount;
    std::uint16_t contentionLastUpdatePos;
public:
    static constexpr uint32_t CONTENTION_INC_THRESHOLD = (std::minstd_rand::max() + 1) / 16.0;
    static constexpr uint32_t CONTENTION_LIMIT = 32;

public:

    std::uint16_t getContentionLastUpdatePos() {
        return contentionLastUpdatePos;
    }

    bool shouldContentionSplit(bool contended, uint16_t write_pos);

    TagAndDirty() {
        init(Tag::_last, RangeOpCounter{});
    }

    void init(Tag t, RangeOpCounter roc) {
        x = 128 | static_cast<uint8_t>(t);
        contentionCount = 0;
        contentionLastUpdatePos = 0;
        rangeOpCounter.init(roc.count.load(std::memory_order_relaxed));
    }

    Tag tag() { return static_cast<Tag>(x & 127); }

    void set_tag(Tag t) { x = static_cast<uint8_t>(t) | 128; }

    void set_dirty(bool d) { x = x & 127 | static_cast<uint8_t>(d) << 7; }

    bool dirty() { return (x & 128) != 0; }
};
#else

struct TagAndDirty {
private:
    bool is_dirty;
public:
    RangeOpCounter rangeOpCounter;

    TagAndDirty() {}

    void init(Tag t, RangeOpCounter roc) {
        abort();
    }

    Tag tag() { abort(); }

    void set_tag(Tag t) { abort(); }

    void set_dirty(bool d) { is_dirty = d; }

    bool dirty() { return is_dirty; }
};

#endif


#endif //BTREE24_TAG_HPP
