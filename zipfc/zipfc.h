#ifndef BTREE24_ZIPFC_H
#define BTREE24_ZIPFC_H

#include <cstdint>

extern "C" {

void
generate_zipf(uint64_t seed, uint32_t key_count, double zipf_parameter, uint32_t *index_out, uint32_t sample_count);
void generate_workload_e(uint64_t seed, double zipf_parameter, uint32_t base_key_count, uint32_t available_key_count,
                         uint32_t op_count, uint32_t *ops_out);
void generate_rng4(uint64_t seed, uint32_t count, uint32_t *out);
void generate_rng8(uint64_t seed, uint32_t count, uint64_t *out);
}

#endif //BTREE24_ZIPFC_H
