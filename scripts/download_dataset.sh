#!/usr/bin/env bash
# Download the full public dataset, then record its validated input manifest.
source "$(dirname "$0")/common.sh" --mode full "$@"
need curl
python3 "$PROJECT_ROOT/src/python/acquire_dataset.py" --data-dir "$DATA_DIR"
python3 "$PROJECT_ROOT/src/python/input_manifest.py"
