#!/usr/bin/env bash
# Recheck exported bytes and independent graph oracles for a completed run.
source "$(dirname "$0")/common.sh" "$@"
# Validation of persisted Parquet runs as the validate stage before export.
# Revalidation here checks exported bytes and independent graph oracles.
python3 "$PROJECT_ROOT/src/python/analyze_results.py" --run-id "$RUN_ID"
