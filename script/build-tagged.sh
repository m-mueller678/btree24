set -e

if [ "$#" -ne 4 ]; then
    echo "use: build <build_dir> <btree_repo_dir> <config> <log2(pagesize)>"
    exit 1  # Exit with a non-zero status to indicate an error
fi

print_error() {
    echo "ERROR" >&2
}

trap 'print_error' ERR

build_dir=$(realpath "$1")
source_dir=$(realpath "$2")
config="$3"
pagesize=$(echo "2^$4" | bc)

echo build: "$build_dir" >&2
echo source: "$source_dir" >&2
echo config: "$config", page-size: $pagesize >&2

cd "$source_dir"

commit_id=$(git rev-parse HEAD)


if ! git diff-index --quiet HEAD --; then
    commit_id="$commit_id-dirty"
    echo "WARN: dirty git" >&2
fi

cd "$build_dir"

cmake -DCMAKE_C_COMPILER=clang-15 \
  -DCMAKE_CXX_COMPILER=clang++-15 \
  -DCONFIG_VARIANT="$config" \
  -DPAGE_SIZE="$pagesize" \
  "$source_dir" \
   > btree_cmake_log 2>&1
cmake --build . >> btree_cmake_log 2>&1
mv btree24 "btree24-$commit_id"

echo $(pwd)/"btree24-$commit_id"