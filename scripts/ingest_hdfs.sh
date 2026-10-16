#!/usr/bin/env bash
# Validate input identity and place immutable raw files in HDFS.
source "$(dirname "$0")/common.sh" "$@"
need hdfs
[[ "$STORAGE" == hdfs ]] || die 'ingest_hdfs requires HDFS storage'
[[ $(hdfs getconf -confKey fs.defaultFS) == hdfs://* ]] || die 'fs.defaultFS must point to HDFS'
python3 "$PROJECT_ROOT/src/python/input_manifest.py"
manifest="$PROJECT_ROOT/data/manifests/$DATASET_ID.json"

# Reuse existing HDFS data only when its manifest and byte hashes match locally.
if hdfs dfs -test -e "$RAW_URI"; then
  hdfs dfs -cat "$RAW_URI/manifest.json" | python3 "$PROJECT_ROOT/src/python/input_manifest.py" --compare "$manifest"
  for f in flights airports airlines; do
    remote=$(hdfs dfs -cat "$RAW_URI/$f.csv" | sha256sum | cut -d' ' -f1)
    local_hash=$(sha256sum "$DATA_DIR/$f.csv" | cut -d' ' -f1)
    [[ "$remote" == "$local_hash" ]] || die "Existing $f.csv checksum mismatch; choose a new dataset ID"
  done
else
  # A new dataset ID receives all three inputs and its manifest together.
  hdfs dfs -mkdir -p "$RAW_URI"
  for f in flights airports airlines; do hdfs dfs -put "$DATA_DIR/$f.csv" "$RAW_URI/"; done
  hdfs dfs -put "$manifest" "$RAW_URI/manifest.json"
fi

# Preserve HDFS placement and size evidence for the run package.
mkdir -p "$ARTIFACT_DIR/logs"
hdfs dfs -ls -h "$RAW_URI" | tee "$ARTIFACT_DIR/logs/ingestion.txt"
hdfs dfs -du -h "$RAW_URI" | tee -a "$ARTIFACT_DIR/logs/ingestion.txt"
