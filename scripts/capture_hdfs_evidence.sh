#!/usr/bin/env bash
# Capture health and output placement for a completed HDFS run.
source "$(dirname "$0")/common.sh" "$@"
[[ "$STORAGE" == hdfs && -f "$ARTIFACT_DIR/run_manifest.json" ]] || die 'Select an existing HDFS run'

# Confirm the selected manifest belongs to a validated HDFS execution.
python3 - "$ARTIFACT_DIR/run_manifest.json" "$RUN_ID" "$DATASET_ID" <<'PY'
import json, sys
manifest = json.load(open(sys.argv[1]))
assert manifest['run_id'] == sys.argv[2] and manifest['status'] == 'validated'
assert manifest['configuration']['DATASET_ID'] == sys.argv[3]
assert manifest['configuration']['STORAGE'] == 'hdfs'
PY

# Capture storage placement, filesystem health, and the JVM behind each daemon.
{
  date -u +%FT%TZ
  hdfs getconf -confKey fs.defaultFS
  hdfs dfsadmin -report
  hdfs dfs -ls -h "$RAW_URI"
  hdfs dfs -ls "$RUN_URI/clean" "$RUN_URI/results" "$RUN_URI/validation"
  hdfs fsck "$RAW_URI"
  hdfs fsck "$RUN_URI"
  "$HADOOP_JAVA_HOME/bin/java" -version 2>&1
  python3 - <<'PY'
from pathlib import Path

# Inspect /proc directly so evidence names the executable used by each daemon.
for process in Path('/proc').iterdir():
    if not process.name.isdigit():
        continue
    try:
        args = (process / 'cmdline').read_bytes().split(b'\0')
        classes = (b'org.apache.hadoop.hdfs.server.namenode.NameNode', b'org.apache.hadoop.hdfs.server.datanode.DataNode')
        for name in classes:
            if name in args:
                print(name.decode(), 'pid=' + process.name, 'java=' + str((process / 'exe').resolve()))
    except (PermissionError, FileNotFoundError, ProcessLookupError):
        pass
PY
} > "$ARTIFACT_DIR/hdfs-health.txt" 2>&1
echo "Saved HDFS health, file placement, and daemon JVM evidence for $RUN_ID"
