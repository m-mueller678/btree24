#pragma once

#define USE_STRUCTURE_BTREE

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

#ifdef NDEBUG
constexpr bool IS_DEBUG=false;
#else
constexpr bool IS_DEBUG = true;
#endif