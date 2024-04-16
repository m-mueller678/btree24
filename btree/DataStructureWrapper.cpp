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

bool DataStructureWrapper::lookup(std::span<uint8_t> key, std::span<uint8_t> &valueOut) {
    bool found = impl.lookupImpl(key, valueOut);
#ifdef CHECK_TREE_OPS
    auto std_found = std_map.find(toByteVector(key));
    assert(found == (std_found != std_map.end()));
    if (found) {
        auto &std_found_val = std_found->second;
        assert(valueOut.size() == std_found_val.size());
        assert(memcmp(std_found_val.data(), valueOut.data(), valueOut.size()) == 0);
    }
#endif
    return found;
}

void DataStructureWrapper::range_lookup(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                                        const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
#ifdef CHECK_TREE_OPS
    try{
        bool shouldContinue = true;
        auto keyVec = toByteVector(key);
        auto std_iterator = std_map.lower_bound(keyVec);
        impl.range_lookupImpl(key, keyLen, keyOut, [&](unsigned keyLen, std::span<uint8_t> payload) {
            assert(shouldContinue);
            assert(std_iterator != std_map.end());
            assert(std_iterator->first.size() == keyLen);
            assert(memcmp(std_iterator->first.data(), keyOut, keyLen) == 0);
            assert(std_iterator->second.size() == payload.size());
            assert(memcmp(std_iterator->second.data(), payload.data(), payload.size()) == 0);
            shouldContinue = found_record_cb(keyLen, payload, payloadLen);
            ++std_iterator;
            return shouldContinue;
        });
        if (shouldContinue) {
            assert(std_iterator == std_map.end());
        }
    }
#else
    impl.range_lookupImpl(key, keyOutBuffer, std::move(found_record_cb));
#endif
}
