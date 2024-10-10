#define USE_STRUCTURE_BTREE
#define ENABLE_CONTENTION_SPLIT 1
constexpr bool enablePrefix = true;
constexpr bool enableBasicHead = true;
constexpr bool enableDense = true;
constexpr bool enableHash = true;
constexpr unsigned basicHintCount = 16;
constexpr bool enableDense2 = false;
constexpr bool enableHashAdapt = true;
constexpr bool enableDensifySplit = true;