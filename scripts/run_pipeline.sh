#!/usr/bin/env bash
# Orchestrate a fresh end-to-end run without overwriting partial evidence.
source "$(dirname "$0")/common.sh" "$@"
[[ ! -e "$ARTIFACT_DIR" ]] || die "Run $RUN_ID already exists; use a fresh ID (partial runs are preserved)"
mkdir -p "$ARTIFACT_DIR/logs"
status=failed

# The exit trap records failure unless every stage and analysis completes.
finish() { python3 "$PROJECT_ROOT/src/python/run_manifest.py" "$status"; }
trap finish EXIT
python3 "$PROJECT_ROOT/src/python/run_manifest.py" running
python3 "$PROJECT_ROOT/src/python/input_manifest.py"
bash "$PROJECT_ROOT/scripts/check_environment.sh" > "$ARTIFACT_DIR/logs/environment-check.log" 2>&1 || true
if [[ "$STORAGE" == hdfs ]]; then bash "$PROJECT_ROOT/scripts/ingest_hdfs.sh"; fi

# Run materialized Spark stages in dependency order.
for stage in smoke preprocess analytics validate; do
  bash "$PROJECT_ROOT/scripts/run_scala_stage.sh" "$stage"
done
if [[ "$MODE" == fixture ]]; then bash "$PROJECT_ROOT/scripts/run_scala_stage.sh" test; fi

# Export and independently analyze only after persisted validation succeeds.
bash "$PROJECT_ROOT/scripts/export_results.sh"
python3 "$PROJECT_ROOT/src/python/run_manifest.py" running
python3 "$PROJECT_ROOT/src/python/analyze_results.py" --run-id "$RUN_ID"
status=validated
echo "Validated $MODE run: $RUN_ID (storage=$STORAGE)"
