This repository contains suplementary data for our paper "B-Trees Are Back: Engineering Fast and Pageable Node Layouts". The B-Tree implementation contained is to help others reproduce our findings. It is not suitable for production use.

# Adaptive B-Tree integrated into vmcache.
Use `script/build-tagged.sh <build_dir> <btree_repo_dir> <config> <log2(pagesize)>` to build a benchmarking binary, where config is the name of one of the files in the `btree/configs` directory, without file extension.
