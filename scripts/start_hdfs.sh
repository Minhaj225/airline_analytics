#!/usr/bin/env bash
# Start existing HDFS daemons only after validating Java compatibility.
source "$(dirname "$0")/common.sh" "$@"
need hdfs
[[ -n "${HADOOP_JAVA_HOME:-}" && -x "$HADOOP_JAVA_HOME/bin/java" ]] || die 'Set HADOOP_JAVA_HOME to a supported daemon JDK; Hadoop 3.4.x requires JDK 11. No daemon started.'
version=$(hadoop version | head -n 1)
daemon_java=$("$HADOOP_JAVA_HOME/bin/java" -version 2>&1)

# Enforce the supported daemon JDK for the detected Hadoop release line.
case "$version" in
  'Hadoop 3.3.'*|'Hadoop 3.4.'*) [[ "$daemon_java" == *'version "11.'* ]] || die 'This Hadoop line requires daemon JDK 11 for this project';;
  'Hadoop 3.5.'*) [[ "$daemon_java" == *'version "17.'* ]] || die 'Use daemon JDK 17 for Hadoop 3.5';;
  *) die 'Review Java compatibility for this Hadoop version before starting daemons';;
esac
# Existing hadoop-env.sh may override JAVA_HOME; require operator to configure it.
effective=$(JAVA_HOME="$HADOOP_JAVA_HOME" hdfs envvars 2>/dev/null)
printf '%s\n' "$effective" | python3 -c 'import os,re,sys; text=sys.stdin.read(); match=re.search(r"(?m)^JAVA_HOME=[\x27\x22]?([^\x27\x22\n]+)",text); sys.exit(0 if match and os.path.realpath(match[1]) == os.path.realpath(os.environ["HADOOP_JAVA_HOME"]) else 1)' || die 'hadoop-env.sh overrides HADOOP_JAVA_HOME; configure its JAVA_HOME explicitly before startup.'

# Start only missing daemons; this script never initializes or formats storage.
if ! timeout 15 hdfs dfsadmin -report >/dev/null 2>&1; then
  JAVA_HOME="$HADOOP_JAVA_HOME" hdfs --daemon start namenode
fi
if ! timeout 15 hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes ([1-9]'; then
  JAVA_HOME="$HADOOP_JAVA_HOME" hdfs --daemon start datanode
fi

# Wait until a live DataNode is visible and safe mode is off.
for attempt in {1..45}; do
  if timeout 5 hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes ([1-9]' &&
     timeout 5 hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'Safe mode is OFF'; then exit 0; fi
  sleep 1
done
die 'HDFS is not writable (DataNode unavailable or safe mode remains on); inspect daemon logs. This script never formats storage.'
