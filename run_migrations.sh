#!/bin/bash
set -e  # stop on error

MONGO_URI="${MONGO_URI:-mongodb://mongo1:27017/}"
VERBOSITY="-v INFO"

# Step 1: Create collection
python3 migration/create_collection.py --mongodb_address "$MONGO_URI"

# Step 2: Load XLSX data
for dataset in Ichtegem Madeleine; do
  python3 migration/xlsx.py "$dataset" --mongodb_address "$MONGO_URI" $VERBOSITY
done

# Step 3: Load JSONL
python3 migration/jsonl.py InfoClimat --mongodb_address "$MONGO_URI" $VERBOSITY

# Step 4: Run tests
for dataset in Ichtegem Madeleine InfoClimat; do
  pytest -v --input "$dataset"
done
