#pragma once

#define USE_STRUCTURE_BTREE

#include <cstdint>

constexpr bool enablePrefix = true;
constexpr bool enableBasicHead = true;
constexpr bool enableDense = true;
constexpr bool enableHash = true;
constexpr unsigned basicHintCount = 16;
constexpr bool enableDense2 = false;
constexpr bool enableHashAdapt = true;
constexpr bool enableDensifySplit = true;
constexpr const char *configName = "dev_config_name";
constexpr unsigned maxKvSize = 1024;
constexpr unsigned pageSize = 1 << 12;
constexpr unsigned pageSizeLeaf = pageSize;
constexpr unsigned pageSizeInner = pageSize;

#ifdef NDEBUG
constexpr bool IS_DEBUG=false;
#else
constexpr bool IS_DEBUG = true;
#endif

typedef uint32_t HashSimdBitMask;  // maximum page size (in bytes) is 65536
constexpr unsigned hashSimdWidth = sizeof(HashSimdBitMask) * 8;