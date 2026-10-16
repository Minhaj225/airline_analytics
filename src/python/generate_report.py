"""Assemble a truthful selected-run report with full current source listings."""
import argparse
import hashlib
import json
import re
from pathlib import Path
from analyze_results import ROOT, analyze, read_csv

HEADINGS = ['Introduction', 'Literature Survey', 'Problem Statement',
            'Proposed Solution / Architecture', 'Results & Analysis', 'Conclusion']
APPENDICES = ['Appendix A — Complete Reproduction Procedure',
              'Appendix B — Complete Source Code and Configuration',
              'Appendix C — Execution Evidence and Validation',
              'Appendix D — Team Contributions and Demo', 'Appendix E — References']


# Enumerate authored files that must appear verbatim in the report appendix.
def sources():
    result = [ROOT / f for f in ('README.md', '.gitignore', 'requirements.txt')]
    for directory in ('scripts', 'src', 'tests', 'config'):
        result.extend(p for p in sorted((ROOT / directory).rglob('*')) if p.is_file()
                      and '__pycache__' not in p.parts and p.name != 'project.env')
    return sorted(result)


# Choose a fence longer than any backtick run inside the embedded source.
def fenced(text, language='text'):
    length = max([3] + [len(m) + 1 for m in re.findall(r'`+', text)])
    fence = '`' * length
    return f'{fence}{language}\n{text.rstrip()}\n{fence}\n'


# Demote imported document headings so the six required sections stay top-level.
def subordinate(text):
    return re.sub(r'(?m)^(#{1,5}) ', r'##\1 ', text)


# Render dictionaries as a compact Markdown comparison table.
def table(rows, columns):
    def cell(value):
        return str(value).replace('|', '\\|').replace('\n', ' ')
    return '| ' + ' | '.join(columns) + ' |\n| ' + ' | '.join('---' for _ in columns) + ' |\n' + ''.join(
        '| ' + ' | '.join(cell(row.get(c, '')) for c in columns) + ' |\n' for row in rows)


# Remove fenced listings before checking the visible report heading structure.
def visible_markdown(text):
    output = []
    fence = None
    for line in text.splitlines():
        match = re.match(r'^(`{3,})(.*)$', line)
        if fence:
            if line == fence:
                fence = None
        elif match:
            fence = match.group(1)
        else:
            output.append(line)
    if fence:
        raise ValueError('Unbalanced report code fences')
    return '\n'.join(output)


# Verify section order, image links, source hashes, and complete source listings.
def validate_report(path, inventory):
    text = path.read_text()
    visible = visible_markdown(text)
    top = re.findall(r'^# (.+)$', visible, re.M)
    if top != HEADINGS + APPENDICES:
        raise ValueError('Report headings missing, duplicated, or out of order')
    for link in re.findall(r'!\[[^\]]*\]\(([^)]+)\)', visible):
        if not (path.parent / link).is_file():
            raise ValueError('Missing report figure: ' + link)
    for name, digest in inventory.items():
        source = ROOT / name
        if hashlib.sha256(source.read_bytes()).hexdigest() != digest:
            raise ValueError('Obsolete source hash: ' + name)
        if fenced(source.read_text(), source.suffix.lstrip('.') or 'text') not in text:
            raise ValueError('Incomplete source listing: ' + name)


# Assemble the selected-run narrative, evidence, appendices, and integrity manifest.
def generate(run_id):
    analysis = analyze(run_id)
    artifact = ROOT / 'artifacts' / run_id
    export = ROOT / 'data/exported' / run_id
    manifest = json.loads((artifact / 'run_manifest.json').read_text())
    if manifest['status'] != 'validated':
        raise ValueError('Cannot report an incompletely validated run')

    # Require one internally consistent run across figures, metrics, and evidence.
    figures = json.loads((ROOT / 'report/figures' / run_id / 'manifest.json').read_text())
    etl = json.loads((artifact / 'etl.json').read_text())
    weighted = json.loads((artifact / 'weighted_pagerank.json').read_text())
    validation = json.loads((artifact / 'validation.json').read_text())
    if any(value['run_id'] != run_id for value in (manifest, figures, etl, weighted, validation, analysis)):
        raise ValueError('Mixed run IDs')
    expected_figures = {'cleaning', 'monthly', 'pagerank', 'strength', 'clustering', 'reachability', 'delay', 'timings', 'degree_distribution', 'traffic_distribution', 'component_sizes', 'triangles', 'delay_centrality'}
    if {Path(f['file']).stem for f in figures['figures']} != expected_figures or len(figures['figures']) != len(expected_figures):
        raise ValueError('Figure inventory differs from required analyses')
    for entry in figures['figures']:
        path = ROOT / 'report' / entry['file']
        if hashlib.sha256(path.read_bytes()).hexdigest() != entry['sha256']:
            raise ValueError('Figure changed since generation: ' + str(path))
    metrics = json.loads((artifact / 'metrics.json').read_text())
    config = manifest['configuration']
    synthetic = config['MODE'] == 'fixture'
    scope = 'Synthetic fixture results; real airline findings unavailable.' if synthetic else 'Measured results for the retained historical flight network.'
    data_bytes = sum(v['bytes'] for v in manifest['input_manifest']['files'].values())
    ranks = sorted(read_csv(export / 'results/weighted_pagerank.csv'), key=lambda r: int(r['rank']))
    baseline = sorted(read_csv(export / 'results/pagerank.csv'), key=lambda r: int(r['rank']))
    components = read_csv(export / 'results/component_sizes.csv')
    triangles = read_csv(export / 'results/triangle_count.csv')
    paths = read_csv(export / 'results/shortest_paths.csv')
    unreachable = sum(r['reachable'] == 'false' for r in paths)
    numeric = next(int(r['records']) for r in read_csv(export / 'audit/cleaning_funnel.csv') if r['reason'] == 'numeric_airport')
    coverage = etl['delay_observations'] / etl['retained_records']
    mean = etl['sum_arrival_delay'] / etl['delay_observations'] if etl['delay_observations'] else None

    # Build the report in required section order from validated measured values.
    parts = [f'Airline Graph Analytics — CSE412\n\nSelected run: `{run_id}`. **{scope}**\n\n']
    parts.append('# Introduction\n\n' +
        'This project implements the data preparation and graph-analysis stages of an airline network study. '
        'HDFS is the intended file store, Spark SQL processes flight rows, and Scala GraphX computes network metrics. '
        'The research questions concern centrality, weak connectivity, neighborhood clustering, hub reachability, '
        'descriptive delays, exclusions, and processing cost. The implementation preserves repeated flights as traffic, '
        'and separates missing delay values from observed zero delay.\n\n' +
        f'The selected run inspected {data_bytes} input bytes and {etl["raw_records"]} records, retained '
        f'{etl["retained_records"]} flights, and produced {etl["vertices"]} airports and {etl["directed_edges"]} '
        f'directed routes. Its storage mode is `{config["STORAGE"]}` and Spark master is `{config["SPARK_MASTER"]}`. '
        'These measurements do not demonstrate multi-node performance or physical data redundancy. '
        'The package includes source, tests, manifests, aggregate tables, figures, and this report.\n\n')
    parts.append('# Literature Survey\n\n' + subordinate((ROOT / 'docs/literature_survey.md').read_text()) + '\n')
    parts.append('# Problem Statement\n\n' +
        'Vertices represent active airports after cleaning. A directed edge represents one retained origin–destination '
        'pair and carries flight count, delay-observation count, delay sum/mean, and the number delayed at least 15 minutes. '
        'Traffic weights are positive; arrival delay is descriptive and may be negative. Reachability counts flight legs '
        'to a hub and is not a feasible schedule, price, or route sequence.\n\n'
        'The input must have unambiguous airport lookup keys. Invalid dates, unknown flags, cancelled/diverted flights, '
        'numeric/malformed airport IDs, self-loops, and unresolved endpoints must be counted rather than silently lost. '
        'Success requires conservation checks, stable IDs, finite converged rank scores, correct projection and direction, '
        'reloaded output validation, and reproducible evidence. The final course acceptance additionally requires an actual '
        'HDFS run on the acquired US DOT files; local synthetic execution does not satisfy that requirement.\n\n')
    parts.append('# Proposed Solution / Architecture\n\n' + subordinate((ROOT / 'docs/architecture.md').read_text()) + '\n')
    parts.append('## Implementation sequence\n\n' + table([
        {'Step': 'Inspect and ingest', 'Code': 'check_environment.sh, input_manifest.py, ingest_hdfs.sh', 'Purpose / evidence': 'Versions, checksums, quoted CSV parsing; HDFS idempotence compares hashes'},
        {'Step': 'Parse and audit', 'Code': 'Preprocess.scala', 'Purpose / evidence': 'Explicit string schema; ordered reasons and monthly coverage; ambiguous lookups fail'},
        {'Step': 'Build graph tables', 'Code': 'Preprocess.scala', 'Purpose / evidence': 'Sorted active IATA IDs; flight counts and observation-weighted delays; Parquet write'},
        {'Step': 'Reload and analyze', 'Code': 'GraphAnalytics.scala, WeightedPageRank.scala', 'Purpose / evidence': 'Endpoint checks; GraphX partitioning; three rankings; components, triangles, directed paths'},
        {'Step': 'Validate and export', 'Code': 'Validation.scala, export_results.py, analyze_results.py', 'Purpose / evidence': 'Read-back invariants, multipart CSV merge, independent tiny matrix oracle'},
        {'Step': 'Present and package', 'Code': 'plot_results.py, generate_report.py, package_submission.py', 'Purpose / evidence': 'Selected-run hashes; figures; full source listing; portable archive'},
    ], ['Step', 'Code', 'Purpose / evidence']) + '\n')
    parts.append('## Weighted iteration and output interpretation\n\n'
        '`PR_next(v) = alpha/N + (1-alpha) * (sum_u PR(u)*w(u,v)/out_strength(u) + dangling_mass/N)`. '
        'Initialization is uniform. Vertices without incoming messages receive a zero incoming contribution; dangling '
        'mass is reduced separately and redistributed to every vertex. New cached state is materialized before old '
        'vertex state is released, with periodic checkpoints to bound lineage. Edge weights never use delays.\n\n'
        'Triangle projection uses every unordered pair once. Local clustering is zero below degree two, otherwise '
        '`2*T/(k*(k-1))`. Global triangle count divides the sum of local counts by three. Weak component IDs are labels. '
        'The complete airport-by-landmark grid preserves unreachable entries as null hops.\n\n' +
        subordinate((ROOT / 'docs/data_dictionary.md').read_text()) + '\n')
    parts.append('## Operational findings\n\n' + subordinate((ROOT / 'docs/troubleshooting.md').read_text()) + '\n')
    parts.append('# Results & Analysis\n\n**' + scope + '**\n\n')
    parts.append(table([{'Measure': k, 'Observed': v} for k, v in etl.items() if k != 'run_id'], ['Measure', 'Observed']) + '\n')
    safe_config = json.dumps(config, indent=2).replace(str(ROOT), '${PROJECT_ROOT}').replace(str(Path.home()), '${HOME}')
    parts.append('## Parameters and checks\n\n' + fenced(safe_config, 'json') + '\n')
    parts.append(f'{len(validation["checks"])} saved-output checks and {len(analysis["checks"])} export/oracle checks passed. '
        f'Weighted PageRank converged in {weighted["iterations"]} iterations, residual {weighted["residual"]:.6g}, '
        f'and score sum {weighted["score_sum"]:.12g}. The comparison uses deterministic IATA tie breaks: '
        f'Spearman rho {analysis["comparison"]["spearman_rho_iata_tiebreak"]}; top-{analysis["comparison"]["top_n"]} '
        f'overlap {analysis["comparison"]["top_n_overlap"]}.\n\n')
    parts.append((artifact / 'findings.md').read_text() + '\n')
    parts.append('## Weight effects with algorithm held fixed\n\n' + table(
        metrics['comparisons']['uniform_pagerank']['largest_rank_changes'],
        ['iata', 'baseline_rank', 'weighted_rank', 'improvement']) + '\n')
    parts.append('## Degree and traffic distributions\n\n' + table(
        [dict(measure=k, **v) for k, v in metrics['degree_and_traffic'].items()],
        ['measure', 'airports', 'minimum', 'median', 'maximum', 'weighted_pagerank_spearman']) + '\n')
    parts.append('## Monthly data coverage\n\n' + table(metrics['monthly'],
        ['year', 'month', 'raw_records', 'retained_records', 'numeric_excluded_records', 'retained_fraction']) + '\n')
    parts.append('## Highest mean delays among qualifying routes\n\n' + table(
        metrics['delay']['highest_delay_routes'],
        ['origin', 'destination', 'num_flights', 'delay_observations', 'avg_arrival_delay', 'delayed_fraction']) + '\n'
        'The delayed fraction uses observed arrival delays as its denominator and a 15-minute threshold.\n\n')
    graph_coverage = metrics.get('graph_coverage', {})
    parts.append('## Scale and lookup completeness\n\n' +
        f"The input contains {metrics['input_bytes']:,} bytes; the graph reduces {etl['retained_records']:,} retained flights "
        f"to {etl['directed_edges']:,} directed routes ({metrics['retained_records_per_directed_route']:.1f} flights per route on average). "
        f"Active-airport coverage: {graph_coverage.get('active_airports_missing_names', 'unmeasured')} missing names and "
        f"{graph_coverage.get('active_airports_missing_coordinates', 'unmeasured')} missing coordinate pairs. "
        'This demonstrates processing of the raw table, not a claim that the final graph is itself massive.\n\n')
    parts.append('## Rankings\n\n' + table(ranks[:int(config['TOP_N'])], ['rank', 'iata', 'score', 'normalized_score']) + '\n')
    if not synthetic:
        parts.append('## Relation to prior research\n\n'
            'The rank comparison follows the motivation of Guimerà et al. [4] to distinguish network importance '
            'from route count. Their worldwide city network and betweenness measure differ from this directed '
            'US-airport PageRank experiment; these results neither reproduce their numerical rankings nor identify communities.\n\n'
            'The persisted SQL-to-GraphX workflow follows the integration approach discussed by Gonzalez et al. [1]. '
            'The measured timings below describe one machine and do not reproduce distributed benchmarks. '
            'The relationship between centrality and delay is an airport-level descriptive association, with no '
            'weather adjustment, significance test, or causal identification.\n\n')
    parts.append('## Figures\n\n' + ''.join(f"![{f['title']}]({f['file']})\n\n{f['title']}. Source: `{run_id}` aggregate tables.\n\n" for f in figures['figures']))
    timing_rows = [dict(stage=s, materialized_seconds=json.loads((artifact / f'{s}-stage.json').read_text())['seconds'],
                        wrapper_seconds=info['wall_seconds_including_startup']) for s, info in manifest['stages'].items()]
    parts.append('## Timing boundaries\n\n' + table(timing_rows, ['stage', 'materialized_seconds', 'wrapper_seconds']) + '\n'
        'Wrapper time includes process startup and Scala compilation. Stage time includes materialized computation and '
        'writes, but excludes REPL startup. Weighted algorithm compute is separately measured in its JSON; its row in '
        'algorithm_timings measures result materialization and writing after iteration. No statistical performance or '
        'multi-node scalability claim is supported by these measurements.\n\n')
    parts.append('# Conclusion\n\n' +
        ('The selected synthetic run validates software behavior. It does not establish real-airline findings.\n\n' if synthetic else
         'The native HDFS, Spark SQL, and GraphX pipeline processed the acquired 2015 flight files and passed the recorded checks. '
         'Traffic-weighted and topology centrality, connectivity, local structure, directed hub distances, and delay associations '
         'are reported from the same retained network. The monthly exclusions are material to their interpretation.\n\n') +
        'The experiment uses a single machine and cannot demonstrate multi-node speedup or physical replication. '
        'A clean-machine installation is documented but was not executed on a separate computer. Future extensions include '
        'a verified date-specific numeric-ID crosswalk, temporal networks, community detection, weather-adjusted delay models, '
        'and a multi-node evaluation. Replace the editable member names before submission; equal contributions were requested by the team.\n\n')
    parts.append('# ' + APPENDICES[0] + '\n\n' + subordinate((ROOT / 'README.md').read_text()) + '\n')

    # Bind the complete source appendix to hashes calculated during this build.
    inventory = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources()}
    parts.append('# ' + APPENDICES[1] + '\n\n' + table([dict(file=k, sha256=v) for k, v in inventory.items()], ['file', 'sha256']) + '\n')
    for name in inventory:
        path = ROOT / name
        parts.append(f'## {name}\n\n' + fenced(path.read_text(), path.suffix.lstrip('.') or 'text') + '\n')
    parts.append('# ' + APPENDICES[2] + '\n\n')
    for name in ('run_manifest.json', 'etl.json', 'weighted_pagerank.json', 'validation.json', 'analysis.json', 'coverage.json', 'metrics.json', 'environment.txt'):
        evidence = (artifact / name).read_text().replace(str(ROOT), '${PROJECT_ROOT}').replace(str(Path.home()), '${HOME}')
        parts.append(f'## {name}\n\n' + fenced(evidence, 'json' if name.endswith('.json') else 'text') + '\n')
    health = artifact / 'hdfs-health.txt'
    if health.exists():
        parts.append('## HDFS health evidence\n\n' + fenced(health.read_text().replace(str(Path.home()), '${HOME}')) + '\n')
    for name in ('test_evidence.json', 'failure-handling.json', 'repeatability.json'):
        path = ROOT / 'artifacts' / name
        if path.exists():
            evidence = path.read_text().replace(str(ROOT), '${PROJECT_ROOT}').replace(str(Path.home()), '${HOME}')
            parts.append(f'## {name}\n\n' + fenced(evidence, 'json') + '\n')
    parts.append('# ' + APPENDICES[3] + '\n\n' + subordinate((ROOT / 'docs/contributions.md').read_text()) + '\n' +
                 subordinate((ROOT / 'docs/demo_script.md').read_text()) + '\n')
    parts.append('# ' + APPENDICES[4] + '\n\nSee the complete linked bibliography in Literature Survey. '
        'Research, dataset, and official documentation are distinguished there. Source code was authored for this '
        'project; no reference repository code was adapted.\n')
    report = ROOT / 'report/project_report.md'
    report.write_text(''.join(parts))
    validate_report(report, inventory)

    # Record source, documentation, and final-report hashes for packaging checks.
    documents = sorted((ROOT / 'docs').glob('*.md')) + [ROOT / 'guide.md', ROOT / 'plan.md']
    document_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in documents}
    (report.parent / 'source_manifest.json').write_text(json.dumps(dict(run_id=run_id, source_sha256=inventory, document_sha256=document_hashes,
        report_sha256=hashlib.sha256(report.read_bytes()).hexdigest(), full_data_findings=not synthetic), indent=2) + '\n')
    print(report)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--run-id', required=True)
    generate(parser.parse_args().run_id)
