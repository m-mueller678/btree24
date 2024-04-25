cmake_minimum_required(VERSION 3.22)
project(btree24)


set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB CONFIG_HEADERS "btree/configs/*.hpp")
add_executable(btree24 btree/main.cpp
        btree/PerfEvent.hpp
        btree/DataStructureWrapper.cpp
        btree/DataStructureWrapper.hpp
        btree/BTree.cpp
        btree/BTree.hpp
        btree/vmache.cpp
        btree/Tag.cpp
        btree/Tag.hpp
        btree/HashNode.cpp
        btree/HashNode.hpp
        btree/BTreeNode.cpp
        btree/BTreeNode.hpp
        btree/AnyNode.cpp
        btree/AnyNode.hpp
        btree/DenseNode.cpp
        btree/DenseNode.hpp
        btree/nodes.hpp
        btree/SeparatorInfo.cpp
        btree/SeparatorInfo.hpp
        btree/common.hpp
        btree/common.cpp
        ${CONFIG_HEADERS}
        #        btree/tuple.hpp
        #        btree/hot_adapter.hpp
        #        btree/hot_adapter.cpp
        # btree/TlxWrapper.cpp
        # btree/TlxWrapper.hpp
)

add_subdirectory(zipfc)
#add_subdirectory(vendor/hot)
#add_subdirectory(vendor/bp-tree)

#if(CONFIG_VARIANT STREQUAL "bp")
#    include_directories(vendor/bp-tree/btree_tests/ParallelTools)
#    include_directories(vendor/bp-tree/)
#    add_subdirectory(vendor/bp-tree)
#else ()
#    include_directories(vendor/tlx/btree_tests/ParallelTools)
#    include_directories(vendor/tlx/)
#    add_subdirectory(vendor/tlx)
#endif ()

target_link_libraries(btree24 PRIVATE zipfc)
#target_link_libraries(btree24 PRIVATE hot)
# target_link_libraries(btree24 PRIVATE tlx)

find_library(LIBAIO_LIBRARY NAMES aio)
if (NOT LIBAIO_LIBRARY)
    message(FATAL_ERROR "libaio library not found")
endif ()
target_link_libraries(btree24 PRIVATE ${LIBAIO_LIBRARY})


option(check_tree_ops "set CHECK_TREE_OPS" OFF)
IF (check_tree_ops)
    target_compile_definitions(btree24 PRIVATE CHECK_TREE_OPS=1)
ENDIF (check_tree_ops)

option(asan "enable asan" OFF)
IF (asan)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
ENDIF (asan)

if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "You probably want to use clang to compile this, as gcc does not support some SIMD stuff we use in HashLeaf")
endif ()

option(CONFIG_VARIANT "Choose a value for CONFIG_VARIANT.hpp")

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