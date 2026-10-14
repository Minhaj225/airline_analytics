# Progress check-in 1 — 2026-10-13 — original fixture milestone

Historical snapshot: the status below describes the initial implementation.
See [execution_log.md](execution_log.md) for the subsequent HDFS and full-data execution.

Environment discovery, CSV inspection, input hashing, safe ingestion scripts,
and Scala preprocessing are implemented. Two local fixture runs persisted and
reloaded five airports and seven routes from ten retained records. Cleaning
counts reconcile to 21 raw records, including one corrupt record.

HDFS is configured at localhost:9000 but was stopped. Hadoop 3.4.1's configured
daemon JVM is Java 17; no supported JDK 11 was found in the inspected locations.
Existing configuration was preserved. No real dataset was acquired, so real
provenance, HDFS listings, and monthly exclusion findings are still outstanding.

## Completion update — 14 October 2026

The HDFS fixture and two full US DOT runs are now validated. The final network
contains 322 airports and 4,656 directed routes from 5,231,130 retained flights.
All 14 non-timing full-run tables repeat within tolerance 1e-8. Provenance,
monthly exclusions, real figures, complete report and portable packaging are
available. See the execution log for timings, tests, limitations and evidence.
