# Troubleshooting

| Symptom | Diagnosis and recovery |
| --- | --- |
| `Operation not permitted` creating sockets | An execution sandbox blocked Spark/HDFS networking. Run these native tools with the environment's explicit permission mechanism; changing graph code will not resolve it |
| `Connection refused` to localhost:9000 | HDFS daemons are stopped or the URI is wrong. Inspect `hdfs getconf -confKey fs.defaultFS` and daemon logs. Do not format storage |
| Hadoop 3.4.x configured with Java 17 | Configure a supported JDK 11 for daemons; preserve Spark's JDK 17 and existing data paths. The startup helper detects overrides in hadoop-env.sh |
| HDFS exits between automated commands | Keep the launching terminal session alive; some execution environments terminate its child daemons on command exit. This is separate from Hadoop storage health |
| HDFS reports a live DataNode but refuses writes in safe mode | Wait for normal block reports and the safe-mode extension to finish. The startup helper checks both DataNode health and writable state; do not force safe mode off or format data |
| HDFS permission denied | Inspect the configured project base and owner. Use a writable user directory; do not delete or change unrelated HDFS directories |
| CSV header/lookup rejection | Compare manifest columns to the data dictionary. Conflicting or duplicate airport keys are fatal to prevent multiplying flight rows |
| Empty graph | Inspect exclusive cleaning reasons and endpoint lookup coverage. A numeric-ID-heavy subset may contain no eligible IATA records |
| Missing GraphX or Scala type errors | Use the installed Spark distribution's spark-shell and its matching JARs. Do not mix SBT artifacts or replace Hadoop client JARs casually |
| Scala serialization exception | Keep executor closures free of driver-only objects. Serializable config and tuple graph attributes are used in the tested implementation |
| REPL returns zero after compilation errors | The wrapper also rejects compiler diagnostics and requires both an exact run-specific stdout marker and a persisted marker |
| `Stage already attempted` | Use a new run ID. Prior logs and output are retained for diagnosis; stale success must not be accepted |
| `Zero triangles` | It can be correct. Test the reverse-ID triangle fixture before interpreting a dataset. Projection canonicalizes every pair before deduplication |
| Missing landmark | Required hubs fail explicitly. Check input exclusions or deliberately configure a different landmark set and record it |
| PageRank non-convergence | Inspect residual and iteration count; increase cap in a new run, documenting the changed parameters |
| RowBasedKeyValueBatch spill warnings during duplicate diagnosis | Spark may spill wide aggregation data under a small heap. Monitor progress and final status; a warning alone is not an out-of-memory failure |
| Out of memory | Review input size, driver heap, Spark local threads, and partitions. Raw flights are not collected; exact-duplicate diagnosis still requires a wide shuffle |
| WindowExec single-partition warning | Sorting the bounded airport mapping/ranking is intentional. The mapping is capped at 10,000 airports; raw-flight processing remains partitioned |
| ShippableVertexPartitionOps warning | GraphX can realign indexes during joins. Fixture outputs are checked independently; the warning is not itself a correctness failure |
| No routes in delay chart | The default requires 100 flights and at least one delay observation. The tiny fixture intentionally has no qualifying routes |
| Figure or report source hash mismatch | Regenerate figures/report from the selected validated run after editing source; never mix run IDs |

The full-run route count, timings, memory use, HDFS client/server compatibility,
and real cleaning rates are not validated by a local fixture. Preserve that
distinction when presenting results.
