"""Acquire the specified public Kaggle dataset, retaining verifiable provenance."""
import argparse
import hashlib
import json
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

SOURCE = 'https://www.kaggle.com/datasets/usdot/flight-delays'
DOWNLOAD = 'https://www.kaggle.com/api/v1/datasets/download/usdot/flight-delays'
METADATA = 'https://www.kaggle.com/api/v1/datasets/view/usdot/flight-delays'
FILES = ('flights.csv', 'airports.csv', 'airlines.csv')


# Compute file identity without loading large artifacts into memory.
def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


# Download or reuse the archive, validate its identity, and extract allowlisted files.
def acquire(directory, existing_archive=False, metadata_file=None):
    directory.mkdir(parents=True, exist_ok=True)
    if any((directory / name).exists() for name in FILES):
        raise ValueError('CSV inputs already exist; select a new data directory')
    archive = directory / 'flight-delays.zip'
    if not existing_archive:
        if archive.exists():
            raise ValueError('Archive already exists; use --existing-archive after download completes')
        subprocess.run(['curl', '-fLsS', '--retry', '3', '--connect-timeout', '30', DOWNLOAD, '-o', str(archive)], check=True)
    if metadata_file is None:
        metadata_file = directory / 'dataset-metadata.json'
        subprocess.run(['curl', '-fLsS', '--retry', '3', '--max-time', '60', METADATA, '-o', str(metadata_file)], check=True)
    metadata = json.loads(metadata_file.read_text())
    if metadata.get('id') != 810 or metadata.get('ref') != 'usdot/flight-delays':
        raise ValueError('Unexpected dataset identity')

    # Validate archive membership and CRC before creating any CSV output.
    with zipfile.ZipFile(archive) as bundle:
        if set(bundle.namelist()) != set(FILES):
            raise ValueError('Unexpected archive inventory; inspect before extraction')
        if bundle.testzip() is not None:
            raise ValueError('ZIP CRC check failed')
        for name in FILES:
            with bundle.open(name) as source, (directory / name).open('xb') as out:
                while chunk := source.read(1024 * 1024):
                    out.write(chunk)

    # Preserve source, license, version, timestamps, sizes, and hashes together.
    provenance = dict(source_url=SOURCE, download_url=DOWNLOAD, metadata_url=METADATA,
        acquired_at=datetime.fromtimestamp(archive.stat().st_mtime, timezone.utc).isoformat(),
        inspected_at=datetime.now(timezone.utc).isoformat(), dataset_id=810,
        dataset_version=metadata['currentVersionNumber'], publisher=metadata['ownerName'],
        source_last_updated=metadata['lastUpdated'], license=metadata['licenseName'],
        license_url=SOURCE, archive_bytes=archive.stat().st_size, archive_sha256=digest(archive),
        files_sha256={name: digest(directory / name) for name in FILES},
        acquisition_method='Public Kaggle download endpoint; no credentials required for this retrieval')
    (directory / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    print(json.dumps(provenance, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--existing-archive', action='store_true')
    parser.add_argument('--metadata-file', type=Path)
    args = parser.parse_args()
    acquire(args.data_dir, args.existing_archive, args.metadata_file)
