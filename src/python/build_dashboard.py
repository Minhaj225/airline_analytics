"""Build a dependency-free dashboard payload from one validated pipeline run."""

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


# Load trusted local JSON metadata for the selected run.
def read_json(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


# Load exported CSV tables while preserving their textual field values.
def read_csv(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


# Prevent accidental mixing of evidence from different pipeline executions.
def require_run_id(document, run_id, source):
    observed = document.get("run_id")
    if observed != run_id:
        raise ValueError(f"Mixed run IDs in {source}: expected {run_id}, found {observed}")


# Normalize Spark-exported numeric strings for JSON serialization.
def as_int(value):
    return int(float(value))


def as_float(value):
    return float(value)


# Assemble a small, browser-ready payload from validated run artifacts.
def build_payload(run_id):
    artifact = ROOT / "artifacts" / run_id
    exported = ROOT / "data" / "exported" / run_id
    figures_root = ROOT / "report" / "figures" / run_id

    manifest = read_json(artifact / "run_manifest.json")
    metrics = read_json(artifact / "metrics.json")
    validation = read_json(artifact / "validation.json")
    analysis = read_json(artifact / "analysis.json")
    figure_manifest = read_json(figures_root / "manifest.json")

    for document, source in (
        (manifest, "run_manifest.json"),
        (metrics, "metrics.json"),
        (validation, "validation.json"),
        (analysis, "analysis.json"),
        (figure_manifest, "figure manifest"),
    ):
        require_run_id(document, run_id, source)

    if manifest.get("status") != "validated" or not validation.get("passed") or not analysis.get("passed"):
        raise ValueError("Dashboard generation is limited to fully validated runs")

    # Read only bounded aggregate outputs; raw flight rows never enter the dashboard.
    cleaning = sorted(
        read_csv(exported / "audit" / "cleaning_funnel.csv"),
        key=lambda row: as_int(row["step"]),
    )
    timings = read_csv(exported / "metrics" / "algorithm_timings.csv")
    airports = read_csv(exported / "results" / "airport_metrics.csv")
    routes = read_csv(exported / "results" / "route_metrics.csv")

    airport_by_iata = {row["iata"]: row for row in airports}
    top_airports = []

    # Enrich the top weighted ranks with human-readable airport metrics.
    for rank in metrics["top_weighted"][:10]:
        airport = airport_by_iata[rank["iata"]]
        top_airports.append(
            {
                "rank": as_int(rank["rank"]),
                "iata": rank["iata"],
                "name": airport["airport_name"],
                "city": airport["city"],
                "score": as_float(rank["normalized_score"]),
                "incoming_flights": as_int(airport["in_strength"]),
                "outgoing_flights": as_int(airport["out_strength"]),
                "avg_arrival_delay": as_float(airport["avg_arrival_delay"]),
            }
        )

    threshold = as_int(manifest["configuration"]["MIN_ROUTE_FLIGHTS"])
    delay_routes = [
        {
            "origin": row["origin_iata"],
            "destination": row["destination_iata"],
            "flights": as_int(row["num_flights"]),
            "avg_delay": as_float(row["avg_arrival_delay"]),
            "delayed_fraction": as_float(row["delayed_fraction"]),
        }
        for row in routes
        if as_int(row["num_flights"]) >= threshold and row["avg_arrival_delay"]
    ]
    delay_routes.sort(key=lambda row: (-row["avg_delay"], row["origin"], row["destination"]))

    # Verify every referenced figure before exposing it to the browser.
    figures = []
    for figure in figure_manifest["figures"]:
        figure_path = figures_root / Path(figure["file"]).name
        if not figure_path.is_file():
            raise FileNotFoundError(f"Missing dashboard figure: {figure_path}")
        figures.append(
            {
                "title": figure["title"],
                "src": f"../report/figures/{run_id}/{figure_path.name}",
            }
        )

    etl = metrics["etl"]
    config = manifest["configuration"]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run": {
            "id": run_id,
            "mode": metrics["mode"],
            "storage": metrics["storage"],
            "dataset_id": config["DATASET_ID"],
            "status": manifest["status"],
            "spark_master": config["SPARK_MASTER"],
            "graph_partitions": as_int(config["GRAPH_PARTITIONS"]),
            "started_at": manifest["started_at"],
        },
        "summary": {
            "raw_records": etl["raw_records"],
            "retained_records": etl["retained_records"],
            "retained_fraction": metrics["retained_fraction"],
            "airports": etl["vertices"],
            "routes": etl["directed_edges"],
            "input_bytes": metrics["input_bytes"],
            "triangles": metrics["global_triangles"],
            "weak_components": len(metrics["components"]),
            "mean_arrival_delay": metrics["delay"]["mean_minutes"],
            "delay_correlation": metrics["delay"]["airport_correlation"]["spearman_average_ties"],
        },
        "cleaning": [
            {
                "step": as_int(row["step"]),
                "reason": row["reason"].replace("_", " "),
                "records": as_int(row["records"]),
                "remaining": as_int(row["remaining_records"]),
            }
            for row in cleaning
        ],
        "monthly": metrics["monthly"],
        "top_airports": top_airports,
        "reachability": metrics["reachability"],
        "rank_comparison": {
            "spearman": metrics["comparisons"]["uniform_pagerank"]["spearman_average_ties"],
            "top_n": metrics["comparisons"]["uniform_pagerank"]["top_n"],
            "overlap": metrics["comparisons"]["uniform_pagerank"]["top_n_overlap"],
        },
        "delay": {
            "coverage": metrics["delay"]["coverage"],
            "threshold": threshold,
            "qualifying_routes": metrics["delay"]["qualifying_routes"],
            "highest_routes": delay_routes[:8],
        },
        "stages": [
            {
                "name": name,
                "status": info["status"],
                "wall_seconds": info["wall_seconds_including_startup"],
                "materialized_seconds": metrics["stage_timings"][name]["materialized_stage_seconds"],
            }
            for name, info in manifest["stages"].items()
        ],
        "algorithms": [
            {
                "name": row["algorithm"].replace("_", " "),
                "compute_seconds": as_float(row["materialize_seconds"]),
                "write_seconds": as_float(row["write_seconds"]),
            }
            for row in timings
        ],
        "validation": {
            "passed": validation["passed"],
            "saved_output_checks": len(validation["checks"]),
            "export_checks": len(analysis["checks"]),
            "checks": [
                {"check": check["check"], "passed": check["passed"]}
                for check in validation["checks"]
            ],
        },
        "figures": figures,
        "limitations": [
            "October has zero retained rows because numeric airport identifiers are excluded.",
            "Weak components are connectivity groups, not detected communities.",
            "Delay results are descriptive associations and do not establish causal propagation.",
            "The measured timings come from one local machine and do not prove cluster speedup.",
            "The aggregated airport graph is much smaller than the raw flight table.",
        ],
    }


# Parse the requested run, serialize the payload, and write the JavaScript file.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="full-2015-final-001")
    parser.add_argument("--output", type=Path, default=ROOT / "dashboard" / "data.js")
    args = parser.parse_args()

    payload = build_payload(args.run_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, allow_nan=False)
    args.output.write_text(
        f"// Generated, read-only payload for the validated dashboard run.\nwindow.DASHBOARD_DATA = {serialized};\n",
        encoding="utf-8",
    )
    print(f"Dashboard payload written to {args.output}")


if __name__ == "__main__":
    main()
