# Airline Graph Analytics

A reproducible big-data pipeline that models the 2015 US airline network as a directed graph. It uses HDFS for storage, Spark SQL for cleaning and aggregation, and Scala GraphX for PageRank, connectivity, triangle, clustering, reachability, and delay analysis.

The main results use the original US DOT 2015 Flight Delays and Cancellations dataset distributed through Kaggle.
- Original input: 5,819,079 flight records
- Retained after cleaning: 5,231,130 records
- Resulting graph: 322 airports and 4,656 directed routes
- Selected real-data run: full-2015-final-001

A small synthetic fixture was also used, but only for correctness testing. It was not used to produce the project’s main findings.

Important limitation: October records were excluded because that month used numeric airport identifiers, so describe the analysis as the retained 2015 flight network, not complete year-long coverage.

## Tech stack

- Hadoop HDFS
- Apache Spark 3.5 and Scala GraphX
- Python 3.11+ for validation, charts, and report generation
- Bash for pipeline orchestration

## Setup

The project is designed for Linux and requires Java 17 for Spark, Java 11 for Hadoop, Hadoop 3.x, and Spark 3.5.x with Scala 2.13.

```bash
cp config/project.env.example config/project.env
# Edit config/project.env if the installed Java, Hadoop, or Spark paths are not discovered automatically.

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt

bash scripts/check_environment.sh --run-id environment-check
```

## Run

Run the small local fixture first. Run IDs are immutable, so use a new ID for each attempt.

```bash
bash scripts/run_pipeline.sh \
  --mode fixture \
  --storage local \
  --run-id fixture-local-demo
```

To exercise the HDFS storage path:

```bash
bash scripts/start_hdfs.sh
bash scripts/run_pipeline.sh \
  --mode fixture \
  --storage hdfs \
  --run-id fixture-hdfs-demo
```

To download and process the full US DOT dataset:

```bash
bash scripts/start_hdfs.sh
bash scripts/download_dataset.sh --mode full --dataset-id usdot-2015
bash scripts/run_pipeline.sh \
  --mode full \
  --dataset-id usdot-2015 \
  --run-id full-2015-demo
```

Generated results are written to `data/exported/<run-id>/`, run evidence to `artifacts/<run-id>/`, and figures to `report/figures/<run-id>/`.

## Results dashboard

Build the dashboard from a validated run and serve it locally:

```bash
python3 src/python/build_dashboard.py --run-id full-2015-final-001
python3 -m http.server 8000 --bind 127.0.0.1
```

Open [http://localhost:8000/dashboard/](http://localhost:8000/dashboard/). See [guide.md](guide.md) for detailed installation and troubleshooting instructions.
