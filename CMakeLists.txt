cmake_minimum_required(VERSION 3.27)
project(btree24)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(btree24 btree/main.cpp
        btree/PerfEvent.hpp
        btree/btree2024.hpp
        btree/DataStructureWrapper.cpp
        btree/DataStructureWrapper.hpp
        btree/BTree.cpp
        btree/BTree.hpp
        btree/vmache.cpp
        btree/Tag.cpp
        btree/Tag.hpp
)

add_subdirectory(zipfc)
target_link_libraries(btree24 PRIVATE zipfc)