"""Compare equivalent runs, disregarding part order and timing."""
import argparse
import csv
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# Compare non-timing tables after proving inputs, parameters, and source match.
def compare(left, right):
    left_root = ROOT / 'data/exported' / left
    right_root = ROOT / 'data/exported' / right
    manifests = [json.loads((ROOT / 'artifacts' / run / 'run_manifest.json').read_text()) for run in (left, right)]
    if any(m['status'] != 'validated' for m in manifests):
        raise ValueError('Only validated runs can establish repeatability')
    if manifests[0]['input_manifest']['files'] != manifests[1]['input_manifest']['files']:
        raise ValueError('Repeatability requires identical input files')
    ignore = {'RUN_ID', 'RUN_URI', 'DATASET_ID', 'RAW_URI', 'DATA_DIR'}
    configs = [{k: v for k, v in m['configuration'].items() if k not in ignore} for m in manifests]
    if configs[0] != configs[1] or manifests[0].get('algorithm_source_sha256') != manifests[1].get('algorithm_source_sha256'):
        raise ValueError('Repeatability requires identical configuration and algorithm source')

    # Metrics contain timing noise, so compare deterministic result tables only.
    tables = lambda root: {p.relative_to(root) for p in root.rglob('*.csv') if p.relative_to(root).parts[0] != 'metrics'}
    if tables(left_root) != tables(right_root) or not tables(left_root):
        raise ValueError('Export table inventories differ or are empty')
    compared = []
    for path in sorted(left_root.rglob('*.csv')):
        relative = path.relative_to(left_root)
        if relative.parts[0] == 'metrics':
            continue

        # Sort by stable identifier columns before comparing multipart exports.
        other = right_root / relative
        with path.open(newline='') as stream, other.open(newline='') as second:
            rows, rows2 = list(csv.DictReader(stream)), list(csv.DictReader(second))
        key_columns = [k for k in ('vertex_id', 'landmark_id', 'src_id', 'dst_id', 'step', 'reason', 'component_id', 'YEAR', 'MONTH', 'origin_iata', 'destination_iata') if rows and k in rows[0]]
        sort_key = lambda r: tuple(r[k] for k in key_columns)
        rows.sort(key=sort_key); rows2.sort(key=sort_key)
        if len(rows) != len(rows2):
            raise AssertionError(f'{relative}: row count changed')
        for a, b in zip(rows, rows2):
            if a.keys() != b.keys():
                raise AssertionError(f'{relative}: schema changed')
            for key in a:
                if a[key] != b[key]:
                    # Permit only the documented numerical tolerance.
                    try:
                        same = math.isclose(float(a[key]), float(b[key]), rel_tol=1e-8, abs_tol=1e-8)
                    except ValueError:
                        same = False
                    if not same:
                        raise AssertionError(f'{relative} {key}: {a[key]} != {b[key]}')
        compared.append(str(relative))
    result = dict(passed=True, left=left, right=right, abs_tolerance=1e-8, rel_tolerance=1e-8, tables=compared)
    (ROOT / 'artifacts/repeatability.json').write_text(json.dumps(result, indent=2) + '\n')
    print(f'{len(compared)} exported tables equivalent across {left} and {right}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('left'); parser.add_argument('right')
    args = parser.parse_args()
    compare(args.left, args.right)
