cmake_minimum_required(VERSION 3.22)
project(btree24)


set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")

option(check_tree_ops "set CHECK_TREE_OPS" OFF)
option(nosync "nosync" OFF)
option(asan "enable asan" OFF)
option(CONFIG_VARIANT "Choose a value for CONFIG_VARIANT.hpp")

add_executable(btree24 btree/main.cpp
        btree/PerfEvent.hpp
        btree/DataStructureWrapper.cpp
        btree/DataStructureWrapper.hpp
        btree/vmache.hpp
        btree/vmache.cpp
        btree/Tag.hpp
        btree/nodes.hpp
        btree/common.hpp
        btree/common.cpp
        btree/BTree.hpp
        btree/BTree.cpp
        btree/HashNode.cpp
        btree/HashNode.hpp
        btree/BTreeNode.cpp
        btree/BTreeNode.hpp
        btree/AnyNode.cpp
        btree/AnyNode.hpp
        btree/DenseNode.cpp
        btree/DenseNode.hpp
        btree/SeparatorInfo.cpp
        btree/SeparatorInfo.hpp
        btree/Tag.cpp
        btree/vmcache_btree.hpp
        btree/vmcache_btree.cpp
        btree/TlxWrapper.hpp
        btree/TlxWrapper.cpp
        ${CONFIG_HEADERS}
        btree/HotBTreeAdapter.hpp
        btree/tuple.hpp
)

# debug flag for tlx
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_definitions(-DDEBUG=1)
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_definitions(-DDEBUG=0)
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    add_definitions(-DDEBUG=0)
else ()
    message(FATAL_ERROR "unhandled profile")
endif ()

if (CONFIG_VARIANT STREQUAL "tlx")
    target_include_directories(btree24 PRIVATE ${CMAKE_SOURCE_DIR}/vendor/BP-Tree/tlx-plain ${CMAKE_SOURCE_DIR}/vendor/BP-Tree/btree_tests/ParallelTools)
elseif (CONFIG_VARIANT STREQUAL "bpt")
    target_include_directories(btree24 PRIVATE ${CMAKE_SOURCE_DIR}/vendor/BP-Tree/tlx-leafds ${CMAKE_SOURCE_DIR}/vendor/BP-Tree/btree_tests/ParallelTools)
elseif (CONFIG_VARIANT STREQUAL "hot")
    target_include_directories(btree24 PRIVATE ${CMAKE_SOURCE_DIR}/vendor/hot/libs/hot/rowex/include/
            ${CMAKE_SOURCE_DIR}/vendor/hot/libs/hot/commons/include/
            ${CMAKE_SOURCE_DIR}/vendor/hot/libs/idx/content-helpers/include/
    )
    target_link_libraries(btree24 PRIVATE tbb)
    target_sources(btree24 PRIVATE btree/HotBTreeAdapter.cpp)
endif ()

file(GLOB CONFIG_HEADERS "btree/configs/*.hpp")

add_subdirectory(zipfc)

target_link_libraries(btree24 PRIVATE zipfc)
target_include_directories(btree24 PRIVATE ${CMAKE_SOURCE_DIR}/)

find_library(LIBAIO_LIBRARY NAMES aio)
if (NOT LIBAIO_LIBRARY)
    message(FATAL_ERROR "libaio library not found")
endif ()
target_link_libraries(btree24 PRIVATE ${LIBAIO_LIBRARY})

IF (check_tree_ops)
    target_compile_definitions(btree24 PRIVATE CHECK_TREE_OPS=1)
ENDIF (check_tree_ops)

IF (nosync)
    target_compile_definitions(btree24 PRIVATE NOSYNC=1)
ENDIF (nosync)

IF (asan)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
ENDIF (asan)

if (CONFIG_VARIANT STREQUAL "hot")
    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU")
        message(FATAL_ERROR "HOT only works with GCC due to template magic")
    endif ()
else ()
    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "You probably want to use clang to compile this, as gcc does not support some SIMD stuff we use in HashLeaf")
    endif ()
endif ()

if (CONFIG_VARIANT)
    add_definitions(-DBTREE_CMAKE_CONFIG_INCLUDE=\"configs/${CONFIG_VARIANT}.hpp\")
    add_definitions(-DBTREE_CMAKE_CONFIG_NAME=\"${CONFIG_VARIANT}\")
endif ()

option(PAGE_SIZE "page size")
if (PAGE_SIZE)
    add_definitions(-DBTREE_CMAKE_PAGE_SIZE=${PAGE_SIZE})
else ()
    add_definitions(-DBTREE_CMAKE_PAGE_SIZE=4096)
endif ()