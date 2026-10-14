"""Stream input CSV validation and provenance without collecting flight rows."""
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REQUIRED = {
    'flights': 'YEAR MONTH DAY AIRLINE FLIGHT_NUMBER TAIL_NUMBER ORIGIN_AIRPORT DESTINATION_AIRPORT ARRIVAL_DELAY CANCELLED DIVERTED'.split(),
    'airports': 'IATA_CODE AIRPORT CITY STATE COUNTRY LATITUDE LONGITUDE'.split(),
    'airlines': 'IATA_CODE AIRLINE'.split(),
}


def inspect(directory):
    files = {}
    for name, required in REQUIRED.items():
        path = directory / f'{name}.csv'
        with path.open('rb') as binary:
            digest = hashlib.file_digest(binary, 'sha256').hexdigest()
        with path.open(newline='', encoding='utf-8-sig') as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader)
            if len(header) != len(set(header)) or not set(required) <= set(header):
                raise ValueError(f'{path}: missing or duplicate columns')
            count = corrupt = 0
            for row in reader:
                count += 1
                corrupt += len(row) != len(header)
        files[path.name] = dict(bytes=path.stat().st_size, sha256=digest,
                                columns=header, parsed_records=count, width_mismatches=corrupt)
    return files


def main():
    if len(sys.argv) == 3 and sys.argv[1] == '--compare':
        remote = json.load(sys.stdin)
        local = json.loads(Path(sys.argv[2]).read_text())
        if (remote['files'], remote['mode']) != (local['files'], local['mode']):
            raise ValueError('Existing HDFS dataset differs; select a new dataset ID')
        return
    root = Path(os.environ['PROJECT_ROOT'])
    mode = os.environ['MODE']
    manifest = dict(dataset_id=os.environ['DATASET_ID'], mode=mode,
                    inspected_at=datetime.now(timezone.utc).isoformat(),
                    source='synthetic correctness fixture' if mode == 'fixture' else 'https://www.kaggle.com/datasets/usdot/flight-delays',
                    publisher='project authors' if mode == 'fixture' else 'US Department of Transportation (Kaggle distribution)',
                    redistribution='Raw files excluded from submission. Check dataset terms before redistribution.',
                    files=inspect(Path(os.environ['DATA_DIR'])))
    if mode == 'full':
        provenance_file = Path(os.environ['DATA_DIR']) / 'provenance.json'
        if not provenance_file.exists():
            raise ValueError('Full input requires provenance.json from acquire_dataset.py or a documented manual acquisition')
        provenance = json.loads(provenance_file.read_text())
        if provenance['files_sha256'] != {name: item['sha256'] for name, item in manifest['files'].items()}:
            raise ValueError('Acquired input hashes differ from the files being inspected')
        manifest['provenance'] = provenance
    target = root / 'data/manifests' / f"{manifest['dataset_id']}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        previous = json.loads(target.read_text())
        if (previous['mode'], previous['files'], previous.get('provenance')) != (mode, manifest['files'], manifest.get('provenance')):
            raise ValueError('Dataset ID already identifies different input; choose a new ID')
    else:
        target.write_text(json.dumps(manifest, indent=2) + '\n')
    print(target)


if __name__ == '__main__':
    main()
