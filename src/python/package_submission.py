"""Allowlisted source/evidence ZIP with extraction and report-link checks."""
import argparse
import hashlib
import json
import re
import tempfile
import zipfile
import subprocess
import sys
from pathlib import Path
from generate_report import ROOT, sources, validate_report, visible_markdown


def package(run_id):
    report = ROOT / 'report/project_report.md'
    metadata = json.loads((ROOT / 'report/source_manifest.json').read_text())
    if metadata['run_id'] != run_id:
        raise ValueError('Regenerate the report for the selected run before packaging')
    validate_report(report, metadata['source_sha256'])
    for name, digest in metadata.get('document_sha256', {}).items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Documentation changed after report generation: ' + name)
    if hashlib.sha256(report.read_bytes()).hexdigest() != metadata['report_sha256']:
        raise ValueError('Report was changed after validation')
    files = sources() + [ROOT / 'plan.md', ROOT / 'guide.md', report, ROOT / 'report/source_manifest.json']
    files += sorted((ROOT / 'docs').glob('*.md'))
    files += sorted((ROOT / 'report/figures' / run_id).glob('*'))
    files += sorted((ROOT / 'data/exported' / run_id).rglob('*.csv'))
    files += [ROOT / 'data/exported' / run_id / 'export_manifest.json']
    artifact = ROOT / 'artifacts' / run_id
    evidence_names = ['run_manifest.json', 'validation.json', 'analysis.json', 'etl.json',
                      'weighted_pagerank.json', 'uniform_pagerank.json', 'environment.txt',
                      'metrics.json', 'coverage.json', 'findings.md', 'hdfs-health.txt',
                      'graph_correctness.json', 'smoke-stage.json', 'preprocess-stage.json',
                      'analytics-stage.json', 'validate-stage.json', 'test-stage.json']
    evidence = [artifact / name for name in evidence_names if (artifact / name).exists()]
    evidence += [p for p in (ROOT / 'artifacts/failure-handling.json', ROOT / 'artifacts/repeatability.json') if p.exists()]
    if (ROOT / 'artifacts/test_evidence.json').exists():
        evidence.append(ROOT / 'artifacts/test_evidence.json')
    repeat_file = ROOT / 'artifacts/repeatability.json'
    if repeat_file.exists():
        repeated = json.loads(repeat_file.read_text())
        if run_id in (repeated['left'], repeated['right']):
            for other_id in {repeated['left'], repeated['right']} - {run_id}:
                if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', other_id):
                    raise ValueError('Invalid repeat-run identifier')
                other = ROOT / 'artifacts' / other_id
                evidence.extend(other / name for name in ('run_manifest.json', 'validation.json', 'analysis.json',
                    'smoke-stage.json', 'preprocess-stage.json', 'analytics-stage.json', 'validate-stage.json'))
    for name in ('logs/ingestion.txt',):
        if (artifact / name).exists():
            evidence.append(artifact / name)
    archive_path = ROOT / 'artifacts' / f'submission-{run_id}.zip'
    if archive_path.exists():
        raise ValueError('Submission archive already exists; move it aside before rebuilding')
    inventory = {}
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(set(files + evidence)):
            if path.is_symlink() or not path.is_file():
                raise ValueError('Unexpected artifact: ' + str(path))
            relative = str(path.relative_to(ROOT))
            data = path.read_bytes()
            if path in evidence:
                data = data.decode().replace(str(ROOT), '${PROJECT_ROOT}').replace(str(Path.home()), '${HOME}').encode()
            archive.writestr('airline/' + relative, data)
            inventory[relative] = hashlib.sha256(data).hexdigest()
        archive.writestr('airline/SUBMISSION_MANIFEST.json', json.dumps(dict(run_id=run_id, sha256=inventory), indent=2) + '\n')
    with tempfile.TemporaryDirectory() as directory, zipfile.ZipFile(archive_path) as archive:
        archive.extractall(directory)
        extracted = Path(directory) / 'airline'
        for name, digest in inventory.items():
            if hashlib.sha256((extracted / name).read_bytes()).hexdigest() != digest:
                raise ValueError('Archive checksum mismatch: ' + name)
        text = visible_markdown((extracted / 'report/project_report.md').read_text())
        for link in re.findall(r'!\[[^\]]*\]\(([^)]+)\)', text):
            if not (extracted / 'report' / link).is_file():
                raise ValueError('Broken extracted report image: ' + link)
        tested = subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-v'],
                                cwd=extracted, capture_output=True, text=True)
        if tested.returncode:
            raise ValueError('Extracted submission tests failed:\n' + tested.stdout + tested.stderr)
    print(f'{archive_path} ({len(inventory)} files, extraction and portable unit/report tests verified)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--run-id', required=True)
    package(parser.parse_args().run_id)
