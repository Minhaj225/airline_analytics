"""Create or update the immutable evidence manifest for one pipeline run."""
import json
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

KEYS = 'MODE STORAGE RUN_ID DATASET_ID DATA_DIR RAW_URI RUN_URI SPARK_MASTER DRIVER_MEMORY SHUFFLE_PARTITIONS GRAPH_PARTITIONS PR_RESET_PROB BASELINE_PR_ITERATIONS WEIGHTED_PR_TOL WEIGHTED_PR_MAX_ITERATIONS LANDMARK_IATA TOP_N MIN_ROUTE_FLIGHTS'.split()

# Resolve paths supplied by the shell wrapper and retain any existing evidence.
root = Path(os.environ['PROJECT_ROOT'])
folder = Path(os.environ['ARTIFACT_DIR'])
target = folder / 'run_manifest.json'
result = json.loads(target.read_text()) if target.exists() else {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'run_id': os.environ['RUN_ID'], 'configuration': {key: os.environ[key] for key in KEYS},
}
result.update(status=sys.argv[1], updated_at=datetime.now(timezone.utc).isoformat())
result['manifest_version'] = 2

# Capture the exact algorithm source once so later runs cannot rewrite history.
if 'algorithm_source_sha256' not in result:
    paths = sorted((root / 'src/scala').glob('*.scala')) + [root / 'tests/graph_correctness.scala', root / 'scripts/run_scala_stage.sh']
    result['algorithm_source_sha256'] = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
result['stages'] = {p.stem: json.loads(p.read_text()) for p in sorted(folder.glob('*.complete'))}

# Hash all authored execution sources for broader submission provenance.
if 'source_sha256' not in result:
    result['source_sha256'] = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                               for directory in ('src', 'scripts', 'tests')
                               for p in sorted((root / directory).rglob('*'))
                               if p.is_file() and '__pycache__' not in p.parts}
source = root / 'data/manifests' / (os.environ['DATASET_ID'] + '.json')

# Attach the inspected input manifest when it is available.
if source.exists():
    result['input_manifest'] = json.loads(source.read_text())
target.write_text(json.dumps(result, indent=2) + '\n')
