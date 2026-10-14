#!/usr/bin/env bash
source "$(dirname "$0")/common.sh" "$@"
python3 "$PROJECT_ROOT/src/python/export_results.py"
