#!/usr/bin/env bash
# Record software, hardware, filesystem, and HDFS readiness for one run.
source "$(dirname "$0")/common.sh" "$@"
mkdir -p "$ARTIFACT_DIR"
failed=0

# Run a diagnostic without hiding later checks when one command fails.
probe() { echo; echo "> $*"; "$@" || { echo "FAILED: $*"; failed=1; }; }

# Keep the complete probe transcript as reproducible environment evidence.
{
  date -u +%FT%TZ
  uname -a
  cat /etc/os-release
  probe getconf _NPROCESSORS_ONLN
  probe free -h
  probe df -h "$PROJECT_ROOT"
  probe java -version
  probe hadoop version
  probe spark-submit --version
  probe spark-shell --version
  probe python3 --version
  probe hdfs getconf -confKey fs.defaultFS
  probe timeout 20 hdfs dfsadmin -report
  echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
  echo "HADOOP_JAVA_HOME=${HADOOP_JAVA_HOME:-not configured}"
  echo "Spark Hadoop client jars:"
  find "${SPARK_HOME:-/nonexistent}/jars" -maxdepth 1 -name 'hadoop-client-*.jar' -printf '%f\n'
  echo "environment_probe_failed=$failed"
  exit "$failed"
} 2>&1 | tee "$ARTIFACT_DIR/environment.txt"
