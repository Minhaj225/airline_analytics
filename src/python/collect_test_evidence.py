"""Preserve compact fixture evidence separately from a selected full-data run."""
import argparse
import hashlib
import json
from analyze_results import ROOT, analyze


# Revalidate one fixture run and collect only compact correctness evidence.
def collect(run_id):
    folder = ROOT / 'artifacts' / run_id
    manifest = json.loads((folder / 'run_manifest.json').read_text())
    if manifest['configuration']['MODE'] != 'fixture' or manifest['status'] != 'validated':
        raise ValueError('Select a validated fixture run')
    analyze(run_id)
    names = ('validation.json', 'analysis.json', 'graph_correctness.json')
    records = {name: json.loads((folder / name).read_text()) for name in names}
    if any(record.get('run_id') != run_id or record.get('passed') is not True for record in records.values()):
        raise ValueError('Mixed or failed fixture evidence')
    result = dict(run_id=run_id, scope='Synthetic fixture correctness; not real-airline findings',
                  configuration=manifest['configuration'],
                  algorithm_source_sha256=manifest['algorithm_source_sha256'], records=records,
                  evidence_sha256={name: hashlib.sha256((folder / name).read_bytes()).hexdigest() for name in names})
    (ROOT / 'artifacts/test_evidence.json').write_text(json.dumps(result, indent=2) + '\n')
    print('Saved fixture evidence:', run_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    collect(parser.parse_args().run_id)
