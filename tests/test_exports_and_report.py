import csv
import json
import sys
import tempfile
import unittest
import zipfile
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/python'))
from analyze_results import power_iteration, distances_to
from export_results import merge_parts
from input_manifest import inspect
from generate_report import fenced, visible_markdown, validate_report
from summarize_results import average_ranks, correlation
import analyze_results
from acquire_dataset import acquire


class CorrectnessTests(unittest.TestCase):
    def test_correlations_use_average_tied_ranks(self):
        self.assertEqual(average_ranks([7, 3, 7, 9]), [2.5, 1, 2.5, 4])
        self.assertAlmostEqual(correlation([1, 1, 2], [3, 3, 4], ranks=True), 1)
        self.assertIsNone(correlation([1, 1], [2, 3], ranks=True))

    def test_acquisition_rejects_unexpected_archive_members(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            with zipfile.ZipFile(directory / 'flight-delays.zip', 'w') as archive:
                archive.writestr('../escape.csv', 'x')
            metadata = directory / 'metadata.json'
            metadata.write_text(json.dumps(dict(id=810, ref='usdot/flight-delays')))
            with self.assertRaisesRegex(ValueError, 'Unexpected archive inventory'):
                acquire(directory, existing_archive=True, metadata_file=metadata)
            self.assertFalse((directory / 'flights.csv').exists())

    def test_analysis_rejects_mixed_runs_and_incomplete_inventories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / 'artifacts/fixture-integrity'
            artifact.mkdir(parents=True)
            manifest = dict(run_id='fixture-integrity', configuration=dict(MODE='fixture'))
            (artifact / 'run_manifest.json').write_text(json.dumps(manifest))
            (artifact / 'validation.json').write_text(json.dumps(dict(run_id='fixture-integrity', passed=True)))
            exported = root / 'data/exported/fixture-integrity'
            exported.mkdir(parents=True)
            metadata = exported / 'export_manifest.json'
            data = dict(run_id='wrong-run', mode='fixture', sha256={'clean/vertices.csv': 'unused'})
            metadata.write_text(json.dumps(data))
            with patch.object(analyze_results, 'ROOT', root):
                with self.assertRaisesRegex(ValueError, 'Mixed run IDs'):
                    analyze_results.analyze('fixture-integrity')
                data['run_id'] = 'fixture-integrity'
                data['sha256'].pop('clean/vertices.csv')
                metadata.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, 'missing required tables'):
                    analyze_results.analyze('fixture-integrity')

    def test_dense_oracle_cycle_and_dangling(self):
        cycle = power_iteration([1, 2, 3], [(1, 2, 1), (2, 3, 1), (3, 1, 1)])
        self.assertTrue(all(abs(x - 1 / 3) < 1e-12 for x in cycle.values()))
        dangling = power_iteration([1, 2, 3], [(1, 2, 1)])
        self.assertAlmostEqual(sum(dangling.values()), 1)
        self.assertGreater(dangling[2], dangling[1])
        self.assertEqual(distances_to([1, 2, 3], [(1, 2, 1), (2, 3, 1)], 3), {1: 2, 2: 1, 3: 0})
        self.assertEqual(distances_to([1, 2, 3], [(1, 2, 1), (2, 3, 1)], 1), {1: 0})

    def test_parts_keep_quoted_fields_and_single_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            (p / 'part-000.csv').write_text('id,name\n1,"Airport, One"\n')
            (p / 'part-001.csv').write_text('id,name\n2,"Airport\nTwo"\n')
            merge_parts(p, p / 'merged.csv')
            with (p / 'merged.csv').open(newline='') as stream:
                self.assertEqual(list(csv.reader(stream)), [['id', 'name'], ['1', 'Airport, One'], ['2', 'Airport\nTwo']])
            (p / 'part-002.csv').write_text('bad,header\n3,x\n')
            with self.assertRaises(ValueError):
                merge_parts(p, p / 'invalid.csv')

    def test_input_manifest_counts_corruption(self):
        files = inspect(ROOT / 'tests/fixtures')
        self.assertEqual(files['flights.csv']['parsed_records'], 21)
        self.assertEqual(files['flights.csv']['width_mismatches'], 1)
        self.assertEqual(files['airports.csv']['parsed_records'], 6)

    def test_nested_source_fences(self):
        source = '# Code listing\n```python\nprint(1)\n```\n'
        report = '# Introduction\n' + fenced(source, 'markdown') + '\n# Conclusion\n'
        self.assertEqual(visible_markdown(report).strip(), '# Introduction\n\n# Conclusion')
        with self.assertRaises(ValueError):
            visible_markdown('```python\nprint(1)')

    def test_generated_report_integrity_when_available(self):
        metadata = ROOT / 'report/source_manifest.json'
        if not metadata.exists():
            self.skipTest('Generate a selected-run report to check its integrity')
        inventory = json.loads(metadata.read_text())['source_sha256']
        validate_report(ROOT / 'report/project_report.md', inventory)


if __name__ == '__main__':
    unittest.main()
