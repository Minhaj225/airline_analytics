"""Merge every Spark CSV part with one header, then record export byte hashes."""
import csv
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path


# Merge multipart Spark output while enforcing one consistent CSV header.
def merge_parts(source, target):
    parts = sorted(source.glob('part-*.csv'))
    if not parts:
        raise ValueError(f'No CSV parts: {source}')
    target.parent.mkdir(parents=True, exist_ok=True)
    header = None
    with target.open('w', newline='') as out:
        writer = csv.writer(out)
        for part in parts:
            with part.open(newline='') as stream:
                reader = csv.reader(stream)
                candidate = next(reader, None)
                if candidate is None:
                    continue
                if header is None:
                    header = candidate
                    writer.writerow(header)
                if candidate != header:
                    raise ValueError(f'Inconsistent part headers: {part}')
                writer.writerows(reader)
    if header is None:
        raise ValueError(f'Missing headers: {source}')


# Export every available result table and record hashes for later validation.
def main():
    root = Path(os.environ['PROJECT_ROOT'])
    artifact = Path(os.environ['ARTIFACT_DIR'])
    manifest = json.loads((artifact / 'run_manifest.json').read_text())
    config = manifest['configuration']
    run = manifest['run_id']
    output = root / 'data/exported' / run
    if output.exists():
        raise ValueError('Export already exists; refusing overwrite')
    if config['STORAGE'] == 'hdfs':
        # Download only the run-owned result directories from HDFS.
        source = artifact / 'downloaded-store'
        source.mkdir()
        for name in ('csv', 'metrics', 'validation', 'tests'):
            if name == 'tests' and config['MODE'] != 'fixture':
                continue
            subprocess.run(['hdfs', 'dfs', '-get', config['RUN_URI'] + '/' + name, str(source)], check=True)
    else:
        source = artifact / 'local-store'
    validation = json.loads((source / 'validation/validation.json').read_text())
    if validation['run_id'] != run or not validation['passed']:
        raise ValueError('Saved output validation missing or failed')

    # Merge every leaf CSV directory into one portable file.
    for directory in sorted((source / 'csv').rglob('*')):
        if directory.is_dir() and list(directory.glob('part-*.csv')):
            merge_parts(directory, output / directory.relative_to(source / 'csv').with_suffix('.csv'))
    for path in sorted((source / 'metrics').glob('*.json')):
        value = json.loads(path.read_text())
        if value['run_id'] != run:
            raise ValueError('Mixed run IDs')
        shutil.copyfile(path, artifact / path.name)
    shutil.copyfile(source / 'validation/validation.json', artifact / 'validation.json')
    if config['MODE'] == 'fixture':
        shutil.copyfile(source / 'tests/graph_correctness.json', artifact / 'graph_correctness.json')

    # Bind every exported table to its exact bytes in a selected-run manifest.
    hashes = {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sorted(output.rglob('*.csv'))}
    (output / 'export_manifest.json').write_text(json.dumps({'run_id': run, 'mode': config['MODE'], 'sha256': hashes}, indent=2) + '\n')
    print(output)


if __name__ == '__main__':
    main()
