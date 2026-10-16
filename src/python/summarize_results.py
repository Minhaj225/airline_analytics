"""Descriptive analysis of bounded exported graph tables, with explicit denominators."""
import csv
import json
import statistics
from collections import Counter


# Load one bounded CSV export into memory for descriptive analysis.
def rows(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


# Assign average ranks to tied values before Spearman correlation.
def average_ranks(values):
    ordered = sorted(range(len(values)), key=values.__getitem__)
    result = [0.0] * len(values)
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and values[ordered[end]] == values[ordered[start]]:
            end += 1
        for index in ordered[start:end]:
            result[index] = (start + 1 + end) / 2
        start = end
    return result


# Compute Pearson correlation directly or over average tied ranks.
def correlation(x, y, ranks=False):
    if len(x) != len(y) or len(x) < 2 or len(set(x)) < 2 or len(set(y)) < 2:
        return None
    return statistics.correlation(average_ranks(x), average_ranks(y)) if ranks else statistics.correlation(x, y)


# Combine validated exports into metrics consumed by reports and dashboards.
def summarize(root, run_id):
    artifact, export = root / 'artifacts' / run_id, root / 'data/exported' / run_id
    def read_json(name):
        value = json.loads((artifact / name).read_text())
        if value['run_id'] != run_id:
            raise ValueError('Mixed run IDs: ' + name)
        return value

    # Load selected-run identity and result tables before deriving metrics.
    manifest = read_json('run_manifest.json')
    etl = read_json('etl.json')
    config = manifest['configuration']
    vertices = rows(export / 'clean/vertices.csv')
    airports = rows(export / 'results/airport_metrics.csv')
    routes = rows(export / 'results/route_metrics.csv')
    triangles = rows(export / 'results/triangle_count.csv')
    components = rows(export / 'results/component_sizes.csv')
    membership = rows(export / 'results/connected_components.csv')
    paths = rows(export / 'results/shortest_paths.csv')
    ranks = {name: {r['iata']: r for r in rows(export / f'results/{name}.csv')}
             for name in ('pagerank', 'weighted_pagerank', 'uniform_pagerank')}
    labels = sorted(ranks['weighted_pagerank'])
    weighted = [float(ranks['weighted_pagerank'][i]['score']) for i in labels]
    comparisons = {}
    for name in ('pagerank', 'uniform_pagerank'):
        other = [float(ranks[name][i]['score']) for i in labels]
        changes = sorted([dict(iata=i, baseline_rank=int(ranks[name][i]['rank']),
                              weighted_rank=int(ranks['weighted_pagerank'][i]['rank']),
                              improvement=int(ranks[name][i]['rank']) - int(ranks['weighted_pagerank'][i]['rank'])) for i in labels],
                         key=lambda r: (-abs(r['improvement']), r['iata']))
        k = min(int(config['TOP_N']), len(labels))
        top_a = {i for i in labels if int(ranks[name][i]['rank']) <= k}
        top_b = {i for i in labels if int(ranks['weighted_pagerank'][i]['rank']) <= k}
        comparisons[name] = dict(spearman_average_ties=correlation(weighted, other, ranks=True),
                                 top_n=k, top_n_overlap=len(top_a & top_b), largest_rank_changes=changes[:10])
    by_code = {r['iata']: r for r in airports}
    centrality = {}
    for field in ('in_degree', 'out_degree', 'in_strength', 'out_strength'):
        values = [int(by_code[i][field]) for i in labels]
        centrality[field] = dict(airports=len(labels), minimum=min(values), maximum=max(values),
            median=statistics.median(values), weighted_pagerank_spearman=correlation(weighted, values, ranks=True))
    observed = [i for i in labels if by_code[i]['avg_arrival_delay'] != '' and int(by_code[i]['arrival_delay_observations']) > 0]
    delays = [float(by_code[i]['avg_arrival_delay']) for i in observed]
    scores = [float(ranks['weighted_pagerank'][i]['score']) for i in observed]
    threshold = int(config['MIN_ROUTE_FLIGHTS'])
    busy = [dict(origin=r['origin_iata'], destination=r['destination_iata'], num_flights=int(r['num_flights']),
                 delay_observations=int(r['delay_observations']), avg_arrival_delay=float(r['avg_arrival_delay']),
                 delayed_fraction=float(r['delayed_fraction'])) for r in routes
            if int(r['num_flights']) >= threshold and r['avg_arrival_delay'] != '']
    busy.sort(key=lambda r: (-r['avg_arrival_delay'], r['origin'], r['destination']))
    reachability = {}
    for hub in config['LANDMARK_IATA'].split(','):
        subset = [r for r in paths if r['landmark_iata'] == hub]
        hops = Counter(int(r['hops']) for r in subset if r['reachable'] == 'true')
        reachability[hub] = dict(reachable=sum(hops.values()), unreachable=len(subset)-sum(hops.values()),
            maximum_hops=max(hops, default=None), hop_distribution={str(k): v for k, v in sorted(hops.items())},
            unreachable_airports=sorted(r['iata'] for r in subset if r['reachable'] == 'false'))
    component_list = sorted([dict(component_id=int(r['component_id']), airports=int(r['airports']),
        members=sorted(m['iata'] for m in membership if m['component_id'] == r['component_id'])) for r in components],
        key=lambda r: (-r['airports'], r['component_id']))
    monthly = rows(export / 'audit/exclusions_by_month.csv')
    monthly = [dict(year=r['YEAR'], month=r['MONTH'], raw_records=int(r['raw_records']),
                    retained_records=int(r['retained_records']), numeric_excluded_records=int(r['numeric_excluded_records']),
                    retained_fraction=int(r['retained_records']) / int(r['raw_records'])) for r in monthly]
    monthly.sort(key=lambda r: (r['year'], int(r['month']) if r['month'].isdigit() else -1))
    bytes_total = sum(r['bytes'] for r in manifest['input_manifest']['files'].values())
    global_triangles = sum(int(r['triangles']) for r in triangles) // 3
    metrics = dict(run_id=run_id, mode=config['MODE'], storage=config['STORAGE'], etl=etl,
        input_bytes=bytes_total, raw_flights_bytes=manifest['input_manifest']['files']['flights.csv']['bytes'],
        retained_fraction=etl['retained_records']/etl['raw_records'],
        raw_records_per_directed_route=etl['raw_records']/etl['directed_edges'],
        retained_records_per_directed_route=etl['retained_records']/etl['directed_edges'],
        comparisons=comparisons, degree_and_traffic=centrality,
        top_weighted=sorted(ranks['weighted_pagerank'].values(), key=lambda r: int(r['rank']))[:int(config['TOP_N'])],
        components=component_list, largest_component_fraction=component_list[0]['airports']/len(vertices),
        global_triangles=global_triangles, mean_local_clustering=statistics.mean(float(r['clustering']) for r in triangles),
        triangles_top=sorted(triangles, key=lambda r: (-int(r['triangles']), r['iata']))[:int(config['TOP_N'])],
        reachability=reachability, monthly=monthly,
        delay=dict(observations=etl['delay_observations'], traffic_flights=etl['retained_records'],
            coverage=etl['delay_observations']/etl['retained_records'],
            mean_minutes=etl['sum_arrival_delay']/etl['delay_observations'] if etl['delay_observations'] else None,
            airport_correlation=dict(airports=len(observed), pearson=correlation(scores, delays),
                                     spearman_average_ties=correlation(scores, delays, ranks=True)),
            busy_route_threshold=threshold, qualifying_routes=len(busy), highest_delay_routes=busy[:int(config['TOP_N'])]),
        stage_timings={stage: dict(materialized_stage_seconds=read_json(f'{stage}-stage.json')['seconds'],
                                   wrapper_seconds=info['wall_seconds_including_startup']) for stage, info in manifest['stages'].items()},
        weighted_pagerank=read_json('weighted_pagerank.json'), uniform_pagerank=read_json('uniform_pagerank.json'))
    coverage_path = artifact / 'coverage.json'
    if coverage_path.exists():
        metrics['graph_coverage'] = read_json('coverage.json')
    (artifact / 'metrics.json').write_text(json.dumps(metrics, indent=2, allow_nan=False) + '\n')
    return metrics


# Turn the numeric summary into careful, submission-ready findings prose.
def findings(metrics):
    m = metrics
    synthetic = m['mode'] == 'fixture'
    lines = [f'## Selected-run findings: {m["run_id"]}\n',
             '**Synthetic correctness observations only.**\n' if synthetic else '**Measured retained-2015-flight findings.**\n']
    top = m['top_weighted'][:5]
    rho = m['comparisons']['uniform_pagerank']['spearman_average_ties']
    rho_text = f'{rho:.4f}' if rho is not None else 'undefined (constant scores)'
    lines.append('### Centrality and traffic\n\nThe five leading traffic-weighted airports are ' +
        ', '.join(f"{r['iata']} ({float(r['score']):.6f})" for r in top) + '. ' +
        f"Weighted versus uniform-custom PageRank has Spearman correlation {rho_text}, " +
        f"with {m['comparisons']['uniform_pagerank']['top_n_overlap']} shared airports in their top {m['comparisons']['uniform_pagerank']['top_n']}. " +
        'This comparison holds the iterative algorithm and stopping parameters fixed; flight-count transition probabilities are the changed input.\n')
    changes = m['comparisons']['uniform_pagerank']['largest_rank_changes']
    if changes:
        shift = changes[0]
        lines.append(f"The largest absolute rank change is {shift['iata']}: uniform rank {shift['baseline_rank']} versus " +
            f"traffic-weighted rank {shift['weighted_rank']}. Route count and repeated traffic therefore need not assign the same importance.\n")
    traffic_rho = m['degree_and_traffic']['in_strength']['weighted_pagerank_spearman']
    degree_rho = m['degree_and_traffic']['in_degree']['weighted_pagerank_spearman']
    lines.append(f'Across active airports, weighted-rank association with incoming traffic is {traffic_rho}; '
                 f'with distinct incoming routes it is {degree_rho} (Spearman, average tied ranks).\n')
    lines.append('### Connectivity and neighborhood structure\n\n' +
        f"The graph contains {len(m['components'])} weak component(s); the largest contains {m['largest_component_fraction']:.1%} of active airports. " +
        f"The undirected projection contains {m['global_triangles']:,} triangles, and the mean per-airport local clustering coefficient is {m['mean_local_clustering']:.4f}. " +
        'A component measures weak connectivity, whereas triangles measure local neighbor interconnection; neither result is a community partition. '
        'A clustering coefficient of one can occur at low-degree airports and must be read alongside degree and raw triangle count.\n')
    lines.append('| Landmark | Reachable airports | Unreachable | Maximum legs |\n| --- | --- | --- | --- |\n' +
                 ''.join(f"| {hub} | {value['reachable']} | {value['unreachable']} | {value['maximum_hops']} |\n" for hub, value in m['reachability'].items()))
    d = m['delay']
    mean_text = f"{d['mean_minutes']:.3f}" if d['mean_minutes'] is not None else 'undefined'
    delay_rho = d['airport_correlation']['spearman_average_ties']
    delay_rho_text = f'{delay_rho:.4f}' if delay_rho is not None else 'undefined'
    lines.append('### Delay denominators and scope\n\n' +
        f"Arrival delay is observed for {d['observations']:,} of {d['traffic_flights']:,} retained flights ({d['coverage']:.2%}); " +
        f"the observation-weighted mean is {mean_text} minutes. " +
        f"At the {d['busy_route_threshold']}-flight threshold, {d['qualifying_routes']:,} routes have an observed average. " +
        f"Airport weighted-centrality versus mean delay uses {d['airport_correlation']['airports']} airports; " +
        f"Spearman correlation (average tied ranks) is {delay_rho_text}. " +
        'This is descriptive association; airport mix, season, weather and scheduling are uncontrolled. It does not establish delay propagation.\n')
    worst = sorted(m['monthly'], key=lambda r: (-r['numeric_excluded_records'], r['month']))[0]
    lines.append(f"The largest exclusive numeric-ID exclusion count occurs in {worst['year']}/{worst['month']}: " +
        f"{worst['numeric_excluded_records']:,} records, with {worst['retained_records']:,} of {worst['raw_records']:,} monthly records retained. " +
        'The graph represents retained records, so it must not be described as complete-year service coverage.\n')
    lines.append('All figures and tables derive from this run. Times are measured on one machine; the final graph is much smaller than the raw flight table.\n')
    return '\n'.join(lines)
