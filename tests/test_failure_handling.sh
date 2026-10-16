#!/usr/bin/env bash
# Integration test: a conflicting lookup must fail, not multiply route counts.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
run_id="failure-conflicting-$(date -u +%Y%m%dT%H%M%S)-$$"
input="$root/artifacts/$run_id-input"

# Copy the valid fixture and introduce one deliberate duplicate lookup key.
mkdir -p "$input"
cp "$root/tests/fixtures/"*.csv "$input/"
echo 'ATL,Conflicting Airport,Elsewhere,XX,USA,0,0' >> "$input/airports.csv"

# Success would indicate the join can silently multiply flight rows.
if bash "$root/scripts/run_scala_stage.sh" preprocess --mode fixture --storage local --run-id "$run_id" --data-dir "$input" > "$input/wrapper.log" 2>&1; then
  echo 'ERROR: conflicting lookup unexpectedly succeeded' >&2
  exit 1
fi

# Require both the specific diagnostic and the absence of a completion marker.
grep -q 'Duplicate airport lookup keys' "$root/artifacts/$run_id/logs/preprocess.log"
[[ ! -e "$root/artifacts/$run_id/preprocess.complete" ]]
printf '{"passed":true,"case":"conflicting_lookup_rejected","run_id":"%s"}\n' "$run_id" > "$root/artifacts/failure-handling.json"
echo 'PASS: invalid lookup returns nonzero and emits no completion marker'
