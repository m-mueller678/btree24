#include "Tag.hpp"


thread_local std::minstd_rand rng;

const char *tag_name(Tag tag) {
    switch (tag) {
#define T(L)    \
   case Tag::L: \
      return #L;
        T(Leaf)
        T(Inner)
        T(Dense)
        T(Hash)
        T(Dense2)
#undef T
    }
    abort();
}

bool isInner(Tag t) {
    return t == Tag::Inner;
}

void RangeOpCounter::range_op() {
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

void RangeOpCounter::point_op() {
    bool should_dec = false;
    while (true) {
        auto c = count.load(std::memory_order_relaxed);
        if (std::int8_t(count) > 0) {
            if (!should_dec) {
                should_dec = rng() < POINT_THRESHOLD;
            }
            if (!should_dec) { break; }
            if (should_dec) {
                if (count.compare_exchange_weak(c, c - 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
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

bool TagAndDirty::contentionSplit(bool contended, uint16_t write_pos) {
    uint32_t r = rng();
    if (r <= CONTENTION_SAMPLE_THRESHOLD) { // track updates
        contentionTotal += 1;
        contentionSlow += contended;
        contentionLastUpdate = write_pos;
    }
    if (r <= CONTENTION_PERIOD_THRESHOLD) { // check for contention
        float ratio = (float) (contentionSlow) / contentionTotal;
        if (ratio == 1) { // contention detected
            if (write_pos != lastUpdatePos) { // unnecessary contention
                int splitPos = mid(pos, p.lastUpdatepos);
                btree.splitNodeAtPos(p.pageContent, splitPos);
            }
        }
        p.lastUpdatedPos = -1; // period ends , reset counters
        p.updatesCount = 0;
        p.slowpathCount = 0;
    }
}
