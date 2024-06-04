#!/bin/bash

set -e

if [ "$#" -lt 3 ]; then
    echo "Usage: <build_dir> <btree_repo_dir> <log2(pagesize)>  <config>*"
    exit 1
fi

# Assign the first three arguments to variables
build_dir="$1"
btree_repo_dir="$2"
pagesize="$3"

# Shift the first three arguments, so we can iterate over the remaining ones
shift 3

if [ "$#" -eq 0 ]; then
    set -- baseline prefix heads hints hash dense2 dense3 adapt vmcache tlx wh
fi

echo "$@"

# Loop over the remaining arguments
for config in "$@"; do
    # Invoke the target script with the first three arguments and the current additional argument
    "$(dirname "$0")/build-tagged.sh" "$build_dir" "$btree_repo_dir"  "$config" "$pagesize"
done
