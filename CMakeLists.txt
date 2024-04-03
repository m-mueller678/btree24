cmake_minimum_required(VERSION 3.27)
project(btree24)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
)

add_subdirectory(zipfc)
target_link_libraries(btree24 PRIVATE zipfc)

find_library(LIBAIO_LIBRARY NAMES aio)
if (NOT LIBAIO_LIBRARY)
    message(FATAL_ERROR "libaio library not found")
endif ()
target_link_libraries(btree24 PRIVATE ${LIBAIO_LIBRARY})