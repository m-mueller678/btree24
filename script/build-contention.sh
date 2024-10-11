#!/bin/bash

# Define the possible values for CONFIG and SIZE
configs=("adapt" "contentionSplit")
sizes=$(seq 9 14)

# Loop through all combinations of CONFIG and SIZE
for config in "${configs[@]}"; do
  for size in $sizes; do
    script/build-tagged.sh tmp . "$config" "$size"
  done
done
