# Proposal

Build a native HDFS → Spark SQL → Scala GraphX pipeline for US DOT's 2015 flight
records. Treat active airports as vertices and completed observed route pairs
as directed edges. Compare topology and traffic-weighted centrality, weak
connectivity, local clustering, directed hub reachability, and descriptive
arrival delays. Audit every exclusion, particularly numeric airport IDs.

Success requires real files in HDFS, persisted and reloaded graph tables,
independently validated algorithms, traceable measured findings, and a
reproducible Markdown report. Synthetic fixtures establish correctness separately from the measured real-data
results. The execution log records which milestones were actually validated.

Suggested sequence: environment and ingestion; ETL and graph construction;
algorithms and validation; analysis/report/demo. Calendar deadlines were not supplied. The team requested equal contributions
with editable member names; see contributions.md. Scope excludes cloud deployment, a web app,
passenger-demand inference, causal delay claims, and itinerary optimization.
