#!/usr/bin/env bash
# Build and verify the portable allowlisted submission archive.
source "$(dirname "$0")/common.sh" "$@"
python3 "$PROJECT_ROOT/src/python/package_submission.py" --run-id "$RUN_ID"
