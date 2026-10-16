"""Generate measured figures from one validated export; no raw flight processing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
from collections import Counter
from analyze_results import ROOT, analyze, read_csv

os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'artifacts/matplotlib-cache'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


# Render all selected-run figures and bind them to source-table hashes.
def plot(run_id):
    analyze(run_id)
    artifact = ROOT / 'artifacts' / run_id
    manifest = json.loads((artifact / 'run_manifest.json').read_text())
    if manifest['status'] != 'validated':
        raise ValueError('Only validated runs may produce figures')
    config = manifest['configuration']
    export = ROOT / 'data/exported' / run_id
    output = ROOT / 'report/figures' / run_id
    output.mkdir(parents=True, exist_ok=True)
    figures = []
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
    color = '#176b87'
    qualifier = 'SYNTHETIC FIXTURE' if config['MODE'] == 'fixture' else 'RETAINED 2015 FLIGHTS'

    # Apply a shared layout, provenance label, and deterministic output manifest.
    def figure(name, title, draw):
        fig, ax = plt.subplots(figsize=(10, 5.8))
        draw(ax)
        ax.set_title(title, loc='left', pad=15)
        fig.suptitle(f'{qualifier} · {run_id}', fontsize=9, color='#666666', y=.985)
        fig.tight_layout(rect=(0, 0, 1, .94))
        path = output / f'{name}.png'
        fig.savefig(path, dpi=160)
        plt.close(fig)
        figures.append(dict(file=str(path.relative_to(ROOT / 'report')), title=title,
                            sha256=hashlib.sha256(path.read_bytes()).hexdigest()))

    # Each drawing below answers one report question from validated aggregates.
    funnel = sorted(read_csv(export / 'audit/cleaning_funnel.csv'), key=lambda r: int(r['step']))
    def cleaning(ax):
        ax.barh([r['reason'].replace('_', ' ') for r in funnel][::-1], [int(r['records']) for r in funnel][::-1], color=color)
        ax.set_xlabel('Records assigned exclusively to each reason')
    figure('cleaning', 'Cleaning decisions and retained records', cleaning)

    months = sorted(read_csv(export / 'audit/exclusions_by_month.csv'), key=lambda r: (r['YEAR'], int(r['MONTH']) if r['MONTH'].isdigit() else -1))
    def monthly(ax):
        labels = [f"{r['YEAR'] or '?'}/{r['MONTH'] or '?'}" for r in months]
        x = list(range(len(labels)))
        ax.bar(x, [int(r['raw_records']) for r in months], color='#c6d8df', label='Raw')
        ax.bar(x, [int(r['retained_records']) for r in months], color=color, label='Retained')
        ax.plot(x, [int(r['numeric_excluded_records']) for r in months], color='#b95036', marker='o', label='Numeric IDs excluded')
        ax.set_xticks(x, labels, rotation=45); ax.set_ylabel('Records'); ax.legend()
    figure('monthly', 'Monthly retention and numeric-ID exclusions', monthly)

    ranks = {name: {r['iata']: r for r in read_csv(export / f'results/{name}.csv')}
             for name in ('pagerank', 'weighted_pagerank', 'uniform_pagerank')}
    top = sorted(ranks['weighted_pagerank'], key=lambda i: int(ranks['weighted_pagerank'][i]['rank']))[:int(config['TOP_N'])]
    def ranking(ax):
        for index, name in enumerate(ranks):
            ax.bar([i + (index - 1) * .25 for i in range(len(top))],
                   [float(ranks[name][i]['normalized_score']) for i in top], width=.25,
                   label=name.replace('_', ' '))
        ax.set_xticks(range(len(top)), top, rotation=45); ax.set_ylabel('Score normalized to sum 1'); ax.legend()
    figure('pagerank', 'Topology, traffic-weighted, and uniform PageRank', ranking)

    airports = read_csv(export / 'results/airport_metrics.csv')
    def strength(ax):
        ax.scatter([int(r['in_strength']) for r in airports], [float(ranks['weighted_pagerank'][r['iata']]['score']) for r in airports], color=color)
        for r in airports:
            if r['iata'] in top[:5]:
                ax.annotate(r['iata'], (int(r['in_strength']), float(ranks['weighted_pagerank'][r['iata']]['score'])), xytext=(4, 4), textcoords='offset points')
        ax.set_xlabel('Incoming flights'); ax.set_ylabel('Traffic-weighted PageRank')
    figure('strength', 'Incoming traffic and weighted centrality', strength)

    def distribution(ax, fields, xlabel):
        ax.hist([[int(r[field]) for r in airports] for field in fields], bins=min(20, max(3, len(airports)//10)),
                label=[f.replace('_', ' ') for f in fields], color=[color, '#b95036'])
        ax.set_xlabel(xlabel); ax.set_ylabel('Airports'); ax.legend()
    figure('degree_distribution', 'Distribution of distinct route connections',
           lambda ax: distribution(ax, ('in_degree', 'out_degree'), 'Distinct neighboring airports'))
    figure('traffic_distribution', 'Distribution of airport flight traffic',
           lambda ax: distribution(ax, ('in_strength', 'out_strength'), 'Retained flights'))

    components = read_csv(export / 'results/component_sizes.csv')
    triangles = read_csv(export / 'results/triangle_count.csv')
    def clustering(ax):
        labels = [r['iata'] for r in sorted(triangles, key=lambda r: (-float(r['clustering']), r['iata']))[:20]]
        by_code = {r['iata']: r for r in triangles}
        ax.bar(labels, [float(by_code[i]['clustering']) for i in labels], color=color)
        ax.set_ylim(0, 1.2); ax.set_ylabel('Local clustering coefficient'); ax.tick_params(axis='x', rotation=45)
        ax.text(.98, .95, f"Weak components: {len(components)}\nGlobal triangles: {sum(int(r['triangles']) for r in triangles) // 3}", ha='right', va='top', transform=ax.transAxes)
    figure('clustering', 'Local clustering and weak connectivity', clustering)

    def component_plot(ax):
        counts = Counter(int(r['airports']) for r in components)
        ax.bar([str(k) for k in sorted(counts)], [counts[k] for k in sorted(counts)], width=.45, color=color)
        ax.set_xlabel('Airports in component'); ax.set_ylabel('Number of weak components')
        ax.yaxis.get_major_locator().set_params(integer=True)
    figure('component_sizes', 'Weak component size distribution', component_plot)
    def triangle_plot(ax):
        selected = sorted(triangles, key=lambda r: (-int(r['triangles']), r['iata']))[:20]
        ax.bar([r['iata'] for r in selected], [int(r['triangles']) for r in selected], color=color)
        ax.set_ylabel('Triangles touching the airport'); ax.tick_params(axis='x', rotation=45)
    figure('triangles', 'Airports with the most undirected triangles', triangle_plot)

    paths = read_csv(export / 'results/shortest_paths.csv')
    def reachability(ax):
        labels = config['LANDMARK_IATA'].split(',')
        bins = sorted({int(r['hops']) for r in paths if r['hops']})
        categories = [str(h) for h in bins] + ['Unreachable']
        width = .8 / len(labels)
        for index, label in enumerate(labels):
            subset = [r for r in paths if r['landmark_iata'] == label]
            counts = Counter(int(r['hops']) for r in subset if r['hops'])
            values = [counts[h] for h in bins] + [sum(r['reachable'] == 'false' for r in subset)]
            ax.bar([x + (index-(len(labels)-1)/2)*width for x in range(len(categories))], values, width=width, label=label)
        ax.set_xticks(range(len(categories)), categories)
        ax.set_xlabel('Flight legs to landmark (zero includes the landmark itself)')
        ax.set_ylabel('Airports'); ax.legend()
    figure('reachability', 'Directed flight legs TO each landmark', reachability)

    routes = read_csv(export / 'results/route_metrics.csv')
    threshold = int(config['MIN_ROUTE_FLIGHTS'])
    displayed = [r for r in routes if int(r['num_flights']) >= threshold and r['avg_arrival_delay']]
    def delays(ax):
        if displayed:
            ax.scatter([int(r['num_flights']) for r in displayed], [float(r['avg_arrival_delay']) for r in displayed], color=color, alpha=.65)
            highest = sorted(displayed, key=lambda r: (-float(r['avg_arrival_delay']), r['origin_iata'], r['destination_iata']))[:5]
            label = 'Highest route means (observed delays)\n' + '\n'.join(
                f"{r['origin_iata']} → {r['destination_iata']}: {float(r['avg_arrival_delay']):.1f} min, n={int(r['delay_observations']):,}" for r in highest)
            ax.text(.98, .97, label, ha='right', va='top', transform=ax.transAxes, fontsize=9,
                    bbox=dict(facecolor='white', edgecolor='#dddddd', alpha=.95, pad=6))
        else:
            ax.text(.5, .5, f'No routes meet the {threshold}-flight threshold\nwith an observed average delay.', ha='center', va='center', transform=ax.transAxes)
        ax.set_xlabel('Route flights'); ax.set_ylabel('Average arrival delay (minutes)')
        ax.axhline(0, color='#aaaaaa', lw=.7)
    figure('delay', f'Route delays: at least {threshold} flights', delays)

    def delay_centrality(ax):
        observed = [r for r in airports if r['avg_arrival_delay']]
        ax.scatter([float(ranks['weighted_pagerank'][r['iata']]['score']) for r in observed],
                   [float(r['avg_arrival_delay']) for r in observed], color=color, alpha=.65)
        ax.axhline(0, color='#aaaaaa', lw=.7)
        ax.set_xlabel('Traffic-weighted PageRank'); ax.set_ylabel('Mean observed arrival delay (minutes)')
        ax.text(.98, .97, f'{len(observed)} airports with observations\nDescriptive association; no causal inference',
                ha='right', va='top', transform=ax.transAxes, fontsize=9)
    figure('delay_centrality', 'Airport centrality and observed arrival delay', delay_centrality)

    stages = manifest['stages']
    def timing(ax):
        labels = [s for s in ('smoke', 'preprocess', 'analytics', 'validate', 'test') if s in stages]
        measured = [json.loads((artifact / f'{s}-stage.json').read_text())['seconds'] for s in labels]
        wall = [stages[s]['wall_seconds_including_startup'] for s in labels]
        ax.bar(labels, wall, color='#c6d8df', label='Wrapper wall time, including startup')
        ax.bar(labels, measured, color=color, label='Materialized stage + writes')
        ax.set_ylabel('Observed seconds (one run)'); ax.legend()
    figure('timings', 'Measured pipeline durations', timing)
    (output / 'manifest.json').write_text(json.dumps(dict(run_id=run_id, mode=config['MODE'], figures=figures), indent=2) + '\n')
    print(f'{len(figures)} figures written to {output}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--run-id', required=True)
    plot(parser.parse_args().run_id)
