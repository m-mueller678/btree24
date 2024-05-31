set -e

GCC_VERSION="${GCC_VERSION:-11}"
CLANG_VERSION="${CLANG_VERSION:-15}"

echo clang: $CLANG_VERSION

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

if [ -n "$NOSYNC" ]; then
    nosync_tag="-nosync"
    nosync_flag="-Dnosync=ON"
else
    nosync_tag=""
    nosync_flag="-Dnosync=OFF"
fi

compiler_flags="-DCMAKE_C_COMPILER=clang-$CLANG_VERSION -DCMAKE_CXX_COMPILER=clang++-$CLANG_VERSION"
if [ "$config" == "hot" ]; then
    compiler_flags="-DCMAKE_C_COMPILER=gcc-$GCC_VERSION -DCMAKE_CXX_COMPILER=g++-$GCC_VERSION"
    rm -r "$build_dir/CMakeFiles"
fi

cmake \
  -DCMAKE_BUILD_TYPE=Release \
  $compiler_flags \
  -DCONFIG_VARIANT="$config" \
  -DPAGE_SIZE="$pagesize" \
  "$nosync_flag" \
  "$source_dir" \
   > btree_cmake_log 2>&1
cmake --build . >> btree_cmake_log 2>&1
bin_name="btree24-$commit_id-$3-$4$nosync_tag"

mv btree24 "$bin_name"

echo $(pwd)/"$bin_name"

if [ "$config" == "hot" ]; then
    rm -r "$build_dir/CMakeFiles"
fi
