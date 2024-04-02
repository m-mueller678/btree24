#ifndef BTREE24_ZIPFC_H
#define BTREE24_ZIPFC_H

#include <cstdint>

extern "C" {
void generate_zipf(uint64_t seed, uint32_t, double, uint32_t *, uint32_t, bool);
void generate_rng4(uint64_t seed, uint32_t count, uint32_t *out);
void generate_rng8(uint64_t seed, uint32_t count, uint64_t *out);
}

#endif //BTREE24_ZIPFC_H
