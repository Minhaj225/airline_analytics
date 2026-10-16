"""Validate aggregate exports; Python is an independent tiny-graph oracle only."""
import argparse
import csv
import hashlib
import itertools
import json
import math
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


# Read a compact exported table as dictionaries for independent validation.
def read_csv(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


# Recompute PageRank with a dense matrix, independently from the GraphX code.
def power_iteration(ids, edges, alpha=0.15, tolerance=1e-13):
    """Independent dense transition matrix including dangling columns."""
    n = len(ids)
    index = {vertex: i for i, vertex in enumerate(ids)}
    matrix = [[0.0] * n for _ in ids]
    strengths = {vertex: sum(w for s, _, w in edges if s == vertex) for vertex in ids}
    for src, dst, weight in edges:
        matrix[index[dst]][index[src]] += weight / strengths[src]
    for src in ids:
        if strengths[src] == 0:
            for row in matrix:
                row[index[src]] = 1 / n
    scores = [1 / n] * n
    for _ in range(10000):
        next_scores = [alpha / n + (1 - alpha) * sum(a * b for a, b in zip(row, scores)) for row in matrix]
        residual = max(abs(a - b) for a, b in zip(scores, next_scores))
        scores = next_scores
        if residual < tolerance:
            return dict(zip(ids, scores))
    raise ValueError('Independent PageRank oracle failed to converge')


# Traverse reversed edges so distances represent paths toward the landmark.
def distances_to(ids, edges, landmark):
    reverse = {v: [] for v in ids}
    for src, dst, _ in edges:
        reverse[dst].append(src)
    result = {landmark: 0}
    queue = deque([landmark])
    while queue:
        current = queue.popleft()
        for predecessor in reverse[current]:
            if predecessor not in result:
                result[predecessor] = result[current] + 1
                queue.append(predecessor)
    return result


# Compare fixture exports against known values and independent graph oracles.
def fixture_checks(export, artifact, config, check):
    expected = json.loads((ROOT / 'tests/expected_fixture_results.json').read_text())
    etl = json.loads((artifact / 'etl.json').read_text())
    for key, value in expected.items():
        if key not in ('reasons', 'ids', 'routes'):
            check('fixture_' + key, etl[key] == value, etl[key])
    actual_ids = {r['iata']: int(r['vertex_id']) for r in read_csv(export / 'clean/vertices.csv')}
    check('fixture_ids', actual_ids == expected['ids'], actual_ids)
    funnel = {r['reason']: int(r['records']) for r in read_csv(export / 'audit/cleaning_funnel.csv')}
    check('fixture_funnel', funnel == expected['reasons'], funnel)
    routes = read_csv(export / 'clean/edges.csv')
    actual_routes = {r['origin_iata'] + '-' + r['destination_iata']:
                     [int(r['num_flights']), int(r['delay_observations']), float(r['sum_arrival_delay']) if r['sum_arrival_delay'] else None]
                     for r in routes}
    check('fixture_routes', actual_routes == expected['routes'], actual_routes)
    ids = sorted(actual_ids.values())
    edges = [(int(r['src_id']), int(r['dst_id']), int(r['num_flights'])) for r in routes]
    for name, weights in [('weighted_pagerank', edges), ('uniform_pagerank', [(s, d, 1) for s, d, _ in edges])]:
        oracle = power_iteration(ids, weights, float(config['PR_RESET_PROB']))
        actual = {int(r['vertex_id']): float(r['score']) for r in read_csv(export / f'results/{name}.csv')}
        difference = max(abs(actual[i] - oracle[i]) for i in ids)
        check(name + '_matrix_oracle', difference < 1e-6, difference)
    pairs = {frozenset((s, d)) for s, d, _ in edges}
    triangles = [t for t in itertools.combinations(ids, 3) if all(frozenset(pair) in pairs for pair in itertools.combinations(t, 2))]
    for row in read_csv(export / 'results/triangle_count.csv'):
        vertex = int(row['vertex_id'])
        count = sum(vertex in t for t in triangles)
        degree = sum(vertex in p for p in pairs)
        coefficient = 2 * count / (degree * (degree - 1)) if degree >= 2 else 0
        check(f'triangle_oracle_{vertex}', count == int(row['triangles']) and degree == int(row['undirected_degree']) and abs(coefficient - float(row['clustering'])) < 1e-10, count)
    for row in read_csv(export / 'results/shortest_paths.csv'):
        distance = distances_to(ids, edges, int(row['landmark_id'])).get(int(row['vertex_id']))
        actual = int(row['hops']) if row['hops'] else None
        check(f"path_oracle_{row['vertex_id']}_{row['landmark_id']}", distance == actual and (distance is not None) == (row['reachable'] == 'true'), actual)
    tests = [
        ('asymmetric_weighted', [(1, 2, 9), (1, 3, 1), (2, 1, 1), (3, 2, 1), (5, 6, 1)]),
        ('asymmetric_uniform', [(1, 2, 1), (1, 3, 1), (2, 1, 1), (3, 2, 1), (5, 6, 1)]),
        ('chain_rank', [(1, 2, 1), (2, 3, 1), (5, 6, 1)]),
    ]
    for name, edges in tests:
        oracle = power_iteration(list(range(1, 7)), edges)
        actual = {int(r['vertex_id']): float(r['score']) for r in read_csv(export / f'tests/{name}.csv')}
        difference = max(abs(actual[i] - oracle[i]) for i in oracle)
        check(name + '_matrix_oracle', difference < 1e-8, difference)


# Validate one selected export and write its analysis and narrative findings.
def analyze(run_id):
    artifact = ROOT / 'artifacts' / run_id
    export = ROOT / 'data/exported' / run_id
    manifest = json.loads((artifact / 'run_manifest.json').read_text())
    exported = json.loads((export / 'export_manifest.json').read_text())
    validation = json.loads((artifact / 'validation.json').read_text())
    if any(m['run_id'] != run_id for m in (manifest, exported, validation)):
        raise ValueError('Mixed run IDs')
    if exported['mode'] != manifest['configuration']['MODE']:
        raise ValueError('Export mode differs from selected run')
    for name, digest in manifest.get('algorithm_source_sha256', {}).items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Algorithm source differs from executed run: ' + name)
    required = {'results/' + name + '.csv' for name in ('pagerank', 'weighted_pagerank', 'uniform_pagerank', 'connected_components', 'component_sizes', 'triangle_count', 'shortest_paths', 'airport_metrics', 'route_metrics')}
    required |= {'clean/vertices.csv', 'clean/edges.csv', 'audit/cleaning_funnel.csv', 'audit/exclusions_by_month.csv', 'audit/unmatched_airports.csv'}
    if not required <= set(exported['sha256']):
        raise ValueError('Export manifest is missing required tables')
    checks = []

    # Store every check with its observed value for auditability.
    def check(name, passed, observed):
        checks.append(dict(check=name, passed=bool(passed), observed=observed))

    for name, digest in exported['sha256'].items():
        check('sha256_' + name, hashlib.sha256((export / name).read_bytes()).hexdigest() == digest, digest)
    check('scala_saved_output_validation', validation['passed'], len(validation['checks']))
    if manifest['configuration']['MODE'] == 'fixture':
        fixture_checks(export, artifact, manifest['configuration'], check)

    # Compare deterministic ranking positions after integrity checks pass.
    ranks = {name: read_csv(export / f'results/{name}.csv') for name in ('pagerank', 'weighted_pagerank', 'uniform_pagerank')}
    baseline = {r['iata']: int(r['rank']) for r in ranks['pagerank']}
    weighted = {r['iata']: int(r['rank']) for r in ranks['weighted_pagerank']}
    n = len(baseline)
    rho = 1 - 6 * sum((baseline[i] - weighted[i]) ** 2 for i in baseline) / (n * (n * n - 1)) if n > 1 else None
    top_n = min(int(manifest['configuration']['TOP_N']), n)
    overlap = len({i for i, r in baseline.items() if r <= top_n} & {i for i, r in weighted.items() if r <= top_n})
    result = dict(run_id=run_id, mode=manifest['configuration']['MODE'], passed=all(c['passed'] for c in checks), checks=checks,
                  comparison=dict(spearman_rho_iata_tiebreak=rho, top_n=top_n, top_n_overlap=overlap,
                                  rank_changes={i: baseline[i] - weighted[i] for i in baseline}),
                  interpretation='Synthetic correctness results only; no real airline findings.' if exported['mode'] == 'fixture' else 'Descriptive historical network; numeric-ID exclusions restrict coverage.')
    (artifact / 'analysis.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    if not result['passed']:
        raise ValueError('Failed checks: ' + ', '.join(c['check'] for c in checks if not c['passed']))

    # Build the richer metrics and prose summaries from the validated tables.
    from summarize_results import summarize, findings
    metrics = summarize(ROOT, run_id)
    (artifact / 'findings.md').write_text(findings(metrics))
    print(f"{len(checks)} export/oracle checks passed for {run_id}")
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    analyze(parser.parse_args().run_id)
