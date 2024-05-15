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

void DataStructureWrapper::lookup(std::span<uint8_t> key, std::function<void(std::span<uint8_t>)> callback) {
#ifdef CHECK_TREE_OPS
    bool found = false;
    auto std_found = std_map.find(toByteVector(key));
    impl.lookupImpl(key, [&](auto value){
        found=true;
        assert(std_found != std_map.end());
        auto &std_found_val = std_found->second;
        assert(value.size() == std_found_val.size());
        assert(memcmp(std_found_val.data(), value.data(), value.size()) == 0);
    });
    if (!found) {
        assert(std_found == std_map.end());
    }
#else
    impl.lookupImpl(key, std::move(callback));
#endif
}

void DataStructureWrapper::range_lookup(std::span<uint8_t> key, uint8_t *keyOutBuffer,
                                        const std::function<bool(unsigned int, std::span<uint8_t>)> &found_record_cb) {
#ifdef CHECK_TREE_OPS
    try{
        bool shouldContinue = true;
        auto keyVec = toByteVector(key);
        auto std_iterator = std_map.lower_bound(keyVec);
        impl.range_lookupImpl(key, keyOutBuffer, [&](unsigned keyLen, std::span<uint8_t> payload) {
            assert(shouldContinue);
            assert(std_iterator != std_map.end());
            assert(std_iterator->first.size() == keyLen);
            assert(memcmp(std_iterator->first.data(), keyOutBuffer, keyLen) == 0);
            assert(std_iterator->second.size() == payload.size());
            assert(memcmp(std_iterator->second.data(), payload.data(), payload.size()) == 0);
            shouldContinue = found_record_cb(keyLen, payload);
            ++std_iterator;
            return shouldContinue;
        });
        if (shouldContinue) {
            assert(std_iterator == std_map.end());
        }
    } catch (OLCRestartException) {
        abort();
    }
#else
    impl.range_lookupImpl(key, keyOutBuffer, std::move(found_record_cb));
#endif
}
