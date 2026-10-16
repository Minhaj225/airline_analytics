# Project guide

This project builds a directed airport graph from the 2015 US flight dataset. HDFS stores the inputs and results, Spark SQL cleans and aggregates the flight records, and Scala GraphX runs the graph algorithms.

The validated reference run is `full-2015-final-001`. It processed 5,819,079 records, retained 5,231,130 flights, and produced 322 airport vertices and 4,656 directed routes.

## Requirements

- Linux
- Bash
- Python 3.11 or newer
- Java 17 for Spark
- Java 11 for Hadoop 3.4.x
- Hadoop 3.x
- Spark 3.5.x with Scala 2.13

The pipeline uses the existing native Hadoop and Spark installations. Do not format an existing NameNode or delete existing HDFS storage while configuring this project.

## Setup

Run the following commands from the repository root:

```bash
cp -n config/project.env.example config/project.env
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Edit `config/project.env` if Java, Hadoop, or Spark cannot be found automatically. Set these values to the installed directories:

```bash
JAVA_HOME=/absolute/path/to/jdk17
HADOOP_JAVA_HOME=/absolute/path/to/jdk11
HADOOP_HOME=/absolute/path/to/hadoop
SPARK_HOME=/absolute/path/to/spark
HADOOP_CONF_DIR=/absolute/path/to/hadoop/etc/hadoop
```

Check the environment:

```bash
bash scripts/check_environment.sh --run-id environment-check
```

The check may report an unavailable HDFS service before it has been started. Fix missing tools or invalid Java paths before continuing.

## Run the local fixture

The local fixture is the fastest correctness check and does not require HDFS:

```bash
bash scripts/run_pipeline.sh \
  --mode fixture \
  --storage local \
  --run-id fixture-local-demo
```

Each run ID is immutable. Use a new ID if you repeat a command.

## Run the fixture with HDFS

Start the existing HDFS installation:

```bash
bash scripts/start_hdfs.sh
hdfs dfsadmin -report
```

Run the fixture through the HDFS storage path:

```bash
bash scripts/run_pipeline.sh \
  --mode fixture \
  --storage hdfs \
  --run-id fixture-hdfs-demo
```

This run uploads the fixture, writes clean Parquet data, reloads it for GraphX, saves the results, and validates the persisted output.

## Run the full dataset

Download and inspect the US DOT dataset:

```bash
bash scripts/download_dataset.sh \
  --mode full \
  --dataset-id usdot-2015
```

Start HDFS if it is not already running, then launch the pipeline with a fresh run ID:

```bash
bash scripts/start_hdfs.sh
bash scripts/run_pipeline.sh \
  --mode full \
  --storage hdfs \
  --dataset-id usdot-2015 \
  --run-id full-2015-demo
```

The full run may take several minutes. Do not reuse the run ID after a failed or interrupted attempt. Logs remain under `artifacts/<run-id>/logs/` for diagnosis.

## Validate and generate outputs

A successful pipeline run already performs saved-output validation and export. These commands rebuild the analysis, figures, report, and submission package when needed:

```bash
python3 src/python/analyze_results.py --run-id full-2015-demo
python3 src/python/plot_results.py --run-id full-2015-demo
python3 src/python/generate_report.py --run-id full-2015-demo
python3 -m unittest discover -s tests -v
bash scripts/package_submission.sh --run-id full-2015-demo
```

Run-specific files are stored in:

- `artifacts/<run-id>/` for manifests, metrics, validation, and logs
- `data/exported/<run-id>/` for compact CSV exports
- `report/figures/<run-id>/` for generated charts

## Open the dashboard

Build the dashboard data from a validated run:

```bash
python3 src/python/build_dashboard.py --run-id full-2015-final-001
python3 -m http.server 8000 --bind 127.0.0.1
```

Open `http://localhost:8000/dashboard/` in a browser. The dashboard only reads saved results and does not modify the pipeline output.

## Inspect HDFS output

Replace the dataset and run IDs if you used different values:

```bash
hdfs dfs -ls -h "/user/${USER}/airline_graph/raw/usdot-2015"
hdfs dfs -ls "/user/${USER}/airline_graph/runs/full-2015-demo/clean"
hdfs dfs -ls "/user/${USER}/airline_graph/runs/full-2015-demo/results"
```

## Stop HDFS

Stop only the local daemons started for this project:

```bash
hdfs --daemon stop datanode
hdfs --daemon stop namenode
```

Stopping the daemons does not remove HDFS data.

## Common problems

### Hadoop uses the wrong Java version

Confirm that `HADOOP_JAVA_HOME` points to JDK 11 and that Hadoop's `hadoop-env.sh` uses the same directory. Keep Spark on JDK 17.

### HDFS is unavailable

Run:

```bash
bash scripts/start_hdfs.sh
hdfs dfsadmin -report
```

Check `artifacts/<run-id>/logs/` if a daemon or pipeline stage fails.

### A run ID already exists

Choose a new run ID. Existing runs are preserved so failed attempts cannot overwrite validated evidence.

### Dashboard figures do not load

Start the web server from the repository root and open the dashboard through the local address. Do not open `dashboard/index.html` directly.

## Demo notes

Use `full-2015-final-001` for the recorded demonstration. Show the architecture, HDFS evidence, cleaning funnel, centrality results, connectivity results, validation checks, and limitations. The detailed recording sequence is in `record_guide.md`.

## Limitations:

- October has no retained rows because numeric airport identifiers are excluded.
- PageRank does not measure safety, profitability, or passenger satisfaction.
- Delay results are descriptive and do not establish causation.
- Weak components are not communities.
- Single-machine timings do not prove cluster speedup.
