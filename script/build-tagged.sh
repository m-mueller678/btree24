set -e

if [ "$#" -ne 2 ]; then
    echo "use: build <build_dir> <btree_repo_dir>"
    exit 1  # Exit with a non-zero status to indicate an error
fi

build_dir=$(realpath "$1")
source_dir=$(realpath "$2")

echo build: "$build_dir" >&2
echo source: "$source_dir" >&2

cd "$source_dir"

commit_id=$(git rev-parse HEAD)


if ! git diff-index --quiet HEAD --; then
    commit_id="$commit_id-dirty"
    echo "WARN: dirty git" >&2
fi

cd "$build_dir"

cmake -DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15 "$source_dir" > btree_cmake_log 2>&1
cmake --build . >> btree_cmake_log 2>&1
mv btree24 "btree24-$commit_id"

echo $(pwd)/"btree24-$commit_id"