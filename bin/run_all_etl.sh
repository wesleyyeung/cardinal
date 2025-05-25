#!/bin/bash

ETL_SCRIPT="./orchestrate_etl.py"
DATASETS_DIR="../datasets"

if [ ! -f "$ETL_SCRIPT" ]; then
  echo "Error: orchestrate_etl.py not found at $ETL_SCRIPT"
  exit 1
fi

echo "Starting ETL for all datasets in $DATASETS_DIR"

for dir in $DATASETS_DIR/*; do
  [ -d "$dir" ] || continue
  dataset=$(basename "$dir")

  # Skip invalid directory names
  if [[ "$dataset" == "*" || "$dataset" =~ [^a-zA-Z0-9_-] ]]; then
    echo "Skipping invalid dataset: $dataset"
    continue
  fi

  echo "Running ETL for dataset: $dataset"
  python "$ETL_SCRIPT" --dataset "$dataset"

  if [ $? -ne 0 ]; then
    echo "ETL failed for $dataset"
  else
    echo "ETL completed for $dataset"
  fi
done

echo "All ETL runs completed."
