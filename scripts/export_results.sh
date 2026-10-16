#!/usr/bin/env bash
# Merge and hash the selected run's persisted CSV result parts.
source "$(dirname "$0")/common.sh" "$@"
python3 "$PROJECT_ROOT/src/python/export_results.py"
