#!/usr/bin/env bash
source "$(dirname "$0")/common.sh" "$@"
python3 "$PROJECT_ROOT/src/python/package_submission.py" --run-id "$RUN_ID"
