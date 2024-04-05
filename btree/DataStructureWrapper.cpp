#include "DataStructureWrapper.hpp"
#include "common.hpp"

DataStructureWrapper::DataStructureWrapper(bool isInt)
        :
#ifdef CHECK_TREE_OPS
        std_map(),
#endif
        impl(isInt) {};


void DataStructureWrapper::testing_update_payload(uint8_t *key, unsigned keyLength, uint8_t *payload) {
#ifdef CHECK_TREE_OPS
    auto found = std_map.find(toByteVector({key, keyLength}));
   memcpy(found->second.data(), payload, found->second.size());
#endif
}

void DataStructureWrapper::insert(std::span<uint8_t> key, std::span<uint8_t> payload) {
#ifdef CHECK_TREE_OPS
    std_map[toByteVector(key)] = toByteVector(payload);
#endif
    return impl.insertImpl(key, payload);
}
