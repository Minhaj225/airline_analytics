#!/usr/bin/env bash
# Shared option parsing, validation, defaults, and paths for every shell entry point.
set -euo pipefail

# Resolve the project root from this file instead of the caller's directory.
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export PROJECT_ROOT
if [[ -z "${AIRLINE_CONFIG_LOADED:-}" && -f "$PROJECT_ROOT/config/project.env" ]]; then
  set -a
  source "$PROJECT_ROOT/config/project.env"
  set +a
fi
export AIRLINE_CONFIG_LOADED=1

# Small guard helpers keep failures explicit and consistent across scripts.
die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || die "Missing prerequisite: $1"; }

# Command-line values override environment defaults loaded above.
MODE=${MODE:-fixture}
STORAGE=${STORAGE:-hdfs}
while (($#)); do
  case "$1" in
    --mode|--storage|--run-id|--dataset-id|--data-dir)
      (($# >= 2)) || die "Missing value for $1"
      case "$1" in
        --mode) MODE=$2;; --storage) STORAGE=$2;; --run-id) RUN_ID=$2;;
        --dataset-id) DATASET_ID=$2;; --data-dir) DATA_DIR=$2;;
      esac
      shift 2;;
    *) die "Unknown option: $1";;
  esac
done
[[ "$MODE" =~ ^(fixture|full)$ ]] || die 'MODE must be fixture or full'
[[ "$STORAGE" =~ ^(hdfs|local)$ ]] || die 'STORAGE must be hdfs or local'
[[ "$STORAGE" != local || "$MODE" == fixture ]] || die 'Local storage is only allowed for fixture development'

# Run and dataset identifiers become paths, so constrain their character set.
RUN_ID=${RUN_ID:-${MODE}-$(date -u +%Y%m%dT%H%M%S)-$$}
DATASET_ID=${DATASET_ID:-$RUN_ID}
for identifier in "$RUN_ID" "$DATASET_ID"; do
  [[ "$identifier" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$ ]] || die "Unsafe identifier: $identifier"
done
[[ "${USER:-}" =~ ^[A-Za-z0-9_-]+$ ]] || die 'Unsafe or missing USER'
HDFS_BASE=${HDFS_BASE:-/user/${USER}/airline_graph}
[[ "$HDFS_BASE" =~ ^/[A-Za-z0-9_/-]+$ && "$HDFS_BASE" != *..* && "$HDFS_BASE" != / ]] || die 'Unsafe HDFS_BASE'
if [[ "$MODE" == fixture ]]; then DATA_DIR=${DATA_DIR:-$PROJECT_ROOT/tests/fixtures};
else DATA_DIR=${DATA_DIR:-$PROJECT_ROOT/data/raw}; fi

# Centralized computational defaults keep every stage and manifest consistent.
SPARK_MASTER=${SPARK_MASTER:-local[2]}
DRIVER_MEMORY=${DRIVER_MEMORY:-2g}
SHUFFLE_PARTITIONS=${SHUFFLE_PARTITIONS:-4}
GRAPH_PARTITIONS=${GRAPH_PARTITIONS:-4}
PR_RESET_PROB=${PR_RESET_PROB:-0.15}
BASELINE_PR_ITERATIONS=${BASELINE_PR_ITERATIONS:-20}
WEIGHTED_PR_TOL=${WEIGHTED_PR_TOL:-1e-8}
WEIGHTED_PR_MAX_ITERATIONS=${WEIGHTED_PR_MAX_ITERATIONS:-100}
LANDMARK_IATA=${LANDMARK_IATA:-ATL,ORD,DFW}
TOP_N=${TOP_N:-20}
MIN_ROUTE_FLIGHTS=${MIN_ROUTE_FLIGHTS:-100}
for number in "$SHUFFLE_PARTITIONS" "$GRAPH_PARTITIONS" "$BASELINE_PR_ITERATIONS" "$WEIGHTED_PR_MAX_ITERATIONS" "$TOP_N" "$MIN_ROUTE_FLIGHTS"; do
  [[ "$number" =~ ^[1-9][0-9]*$ ]] || die "Expected positive integer: $number"
done

# Discover installed runtimes only when explicit configuration is absent.
if [[ -z "${HADOOP_HOME:-}" ]] && command -v hadoop >/dev/null; then HADOOP_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v hadoop)")")"); fi
if [[ -z "${SPARK_HOME:-}" ]] && command -v spark-shell >/dev/null; then SPARK_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v spark-shell)")")"); fi
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-${HADOOP_HOME:-/nonexistent}/etc/hadoop}
ARTIFACT_DIR="$PROJECT_ROOT/artifacts/$RUN_ID"

# Map logical storage locations to HDFS or fixture-only local paths.
if [[ "$STORAGE" == hdfs ]]; then
  RAW_URI="$HDFS_BASE/raw/$DATASET_ID"
  RUN_URI="$HDFS_BASE/runs/$RUN_ID"
else
  RAW_URI="file://$DATA_DIR"
  RUN_URI="file://$ARTIFACT_DIR/local-store"
fi

# Export one stable configuration contract for shell, Python, and Scala stages.
export MODE STORAGE RUN_ID DATASET_ID DATA_DIR HDFS_BASE HADOOP_HOME SPARK_HOME HADOOP_CONF_DIR
export SPARK_MASTER DRIVER_MEMORY SHUFFLE_PARTITIONS GRAPH_PARTITIONS PR_RESET_PROB
export BASELINE_PR_ITERATIONS WEIGHTED_PR_TOL WEIGHTED_PR_MAX_ITERATIONS LANDMARK_IATA
export TOP_N MIN_ROUTE_FLIGHTS ARTIFACT_DIR RAW_URI RUN_URI
