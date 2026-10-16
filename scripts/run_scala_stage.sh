#!/usr/bin/env bash
# Compile and execute one isolated Spark stage with run-specific completion checks.
set -euo pipefail
stage=${1:?Usage: run_scala_stage.sh STAGE [configuration flags]}
shift
source "$(dirname "$0")/common.sh" "$@"
[[ "$stage" =~ ^(preprocess|analytics|validate|test|smoke)$ ]] || die "Unknown stage $stage"
need spark-shell
mkdir -p "$ARTIFACT_DIR/logs"
[[ ! -e "$ARTIFACT_DIR/$stage.complete" && ! -e "$ARTIFACT_DIR/logs/$stage.log" ]] || die "Stage already attempted for $RUN_ID; use a fresh run ID"

# Refuse stale local or HDFS markers before invoking Spark.
if [[ "$STORAGE" == hdfs ]]; then
  [[ $(hdfs getconf -confKey fs.defaultFS) == hdfs://* ]] || die 'Expected HDFS default filesystem'
  if hdfs dfs -test -e "$RUN_URI/markers/$stage.json"; then die 'Stale remote stage marker; use a fresh run ID'; fi
else
  [[ ! -e "$ARTIFACT_DIR/local-store/markers/$stage.json" ]] || die 'Stale local stage marker'
fi
input="$ARTIFACT_DIR/$stage.repl"

# Assemble one REPL compilation unit so partial definitions cannot survive errors.
python3 - "$stage" "$input" <<'PY'
import os, sys
from pathlib import Path
root = Path(os.environ['PROJECT_ROOT'])
# Paste one compilation unit so failed imports/definitions cannot leave stale objects.
files = ['src/scala/ProjectConfig.scala', 'src/scala/Preprocess.scala',
         'src/scala/WeightedPageRank.scala', 'src/scala/GraphAnalytics.scala',
         'src/scala/Validation.scala', 'tests/graph_correctness.scala', 'src/scala/Pipeline.scala']
code = '\n'.join((root / f).read_text() for f in files)
call = 'try { Pipeline.run(spark, "' + sys.argv[1] + '"); println("\\nAIRLINE_STAGE_OK:' + os.environ['RUN_ID'] + ':' + sys.argv[1] + '"); System.exit(0) } catch { case t: Throwable => t.printStackTrace(); System.exit(1) }'
source = Path(sys.argv[2]).with_suffix('.scala')
source.write_text(code)
Path(sys.argv[2]).write_text(':paste ' + str(source) + '\n' + call + '\n:quit\n')
PY

# Capture both the Spark transcript and wall time including REPL startup.
started=$SECONDS
spark-shell --master "$SPARK_MASTER" --driver-memory "$DRIVER_MEMORY" \
  --conf spark.ui.enabled=false --conf spark.sql.warehouse.dir="$ARTIFACT_DIR/warehouse" \
  < "$input" 2>&1 | tee "$ARTIFACT_DIR/logs/$stage.log"
elapsed=$((SECONDS - started))

# Require both a clean transcript and the exact run-specific success message.
if grep -Eq '(^|[[:space:]])error:|[0-9]+ errors? found|org.apache.spark.SparkException:' "$ARTIFACT_DIR/logs/$stage.log"; then die 'Spark reported a compilation or execution failure'; fi
grep -Fxq "AIRLINE_STAGE_OK:$RUN_ID:$stage" "$ARTIFACT_DIR/logs/$stage.log" || die 'Run-specific completion message missing'
if [[ "$STORAGE" == hdfs ]]; then
  hdfs dfs -cat "$RUN_URI/markers/$stage.json" > "$ARTIFACT_DIR/$stage.complete"
else
  cp "$ARTIFACT_DIR/local-store/markers/$stage.json" "$ARTIFACT_DIR/$stage.complete"
fi

# Verify and enrich the persisted stage marker with wrapper wall time.
python3 - "$ARTIFACT_DIR/$stage.complete" "$RUN_ID" "$stage" "$elapsed" <<'PY'
import json, sys
from pathlib import Path
p = Path(sys.argv[1]); marker = json.loads(p.read_text())
assert marker['run_id'] == sys.argv[2] and marker['stage'] == sys.argv[3] and marker['status'] == 'complete'
marker['wall_seconds_including_startup'] = int(sys.argv[4])
p.write_text(json.dumps(marker, indent=2) + '\n')
PY
