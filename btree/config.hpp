#pragma once

#include <cstdint>

typedef uint32_t HashSimdBitMask;  // maximum page size (in bytes) is 65536

#ifdef NDEBUG
constexpr bool IS_DEBUG=false;
#else
constexpr bool IS_DEBUG = true;
#endif

#ifdef BTREE_CMAKE_CONFIG_INCLUDE
#include BTREE_CMAKE_CONFIG_INCLUDE
constexpr const char *configName = BTREE_CMAKE_CONFIG_NAME;
#else
#define USE_STRUCTURE_BTREE
constexpr bool enablePrefix = true;
constexpr bool enableBasicHead = true;
constexpr bool enableDense = false;
constexpr bool enableHash = false;
constexpr unsigned basicHintCount = 16;
constexpr bool enableDense2 = true;
constexpr bool enableHashAdapt = false;
constexpr bool enableDensifySplit = false;
constexpr const char *configName = "dev_config_name";
#define USE_STRUCTURE_BTREE
#endif

constexpr unsigned pageSize = BTREE_CMAKE_PAGE_SIZE;
constexpr unsigned pageSizeLeaf = pageSize;
constexpr unsigned pageSizeInner = pageSize;

constexpr unsigned maxKvSize = (pageSize - 128) / 5;
constexpr unsigned hashSimdWidth = sizeof(HashSimdBitMask) * 8;