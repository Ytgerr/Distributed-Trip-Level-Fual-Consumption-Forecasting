#!/bin/bash
set -euo pipefail

if [ -f "data/VED_171101_week.csv" ]; then
    echo "Dataset already loaded. Skipping download."
else
    echo "Downloading dataset..."
    bash scripts/data_collection.sh
fi

if [ -f "data/vehicles.csv" ]; then
    echo "Vehicle metadata already preprocessed. Skipping preprocessing."
else
    echo "Preprocessing vehicle metadata..."
    python3 scripts/preprocess_dataset.py
fi

echo "Stage 1 prepare data completed."
