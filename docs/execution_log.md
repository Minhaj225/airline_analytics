# Execution log — 2026-10-13

## Implemented

Portable configuration, environment checker, source CSV inspection and hashes,
non-destructive ingestion and guarded daemon startup, Scala preprocessing,
all required GraphX algorithms plus uniform custom PageRank, Parquet reload
validation, multipart export, independent fixture oracles, figures, Markdown
report generation with complete source listings, and allowlisted packaging.

## Executed and validated

- Native inventory: Fedora 44, Temurin Java 17.0.16, Hadoop 3.4.1, Spark 3.5.9,
  Scala 2.13.8, Python 3.14.8. Spark reports Hadoop client 3.3.4.
- `fixture-local-001`: 21 parsed records, 10 retained records, five vertices,
  seven directed edges, seven delay observations, 75 summed delay minutes.
- `fixture-local-002`: repeated the same pipeline with equivalent output.
- Both runs: smoke read/write, preprocessing, GraphX analytics, saved-result
  validation, graph fixtures, export, and 57 Python integrity/oracle checks passed.
- Repeatability: 17 non-timing exported tables agree within absolute/relative
  tolerance 1e-8; IDs are identical.
- Deliberate conflicting airport lookup: wrapper returned nonzero and no
  completion marker; evidence is `artifacts/failure-handling.json`.
- Eight measured fixture figures generated with matplotlib 3.10.8. Ranking and
  reachability images visually inspected. All figures identify synthetic data.

## Issues found and fixed

Initial sandbox probes could not create network sockets. With explicit native
execution permission, Spark ran and HDFS reported connection refused, proving
that HDFS was stopped rather than merely hidden by the sandbox. Existing daemon
settings point to Java 17; the startup helper requires the supported JDK for the
detected Hadoop line and rejects an overriding hadoop-env.sh assignment.

The initial REPL paste through control-D did not terminate reliably. The wrapper
now uses Scala's `:paste <file>` with one compilation unit. Two compile errors
(Column multiplication order and a qualified Scala Properties import) were
fixed before the first passing pipeline. A completion line initially appeared
on the echoed REPL input line; an explicit newline makes exact matching reliable.
The initial attempts remain in ignored artifacts for diagnosis.

## Outstanding at the original fixture milestone (historical)

- A supported Hadoop 3.4.1 daemon JDK must be configured and HDFS started using
  existing storage; no daemon was started or formatted during this work.
- Actual HDFS raw upload, Parquet storage boundary, result persistence, and
  client/server smoke compatibility remain unexecuted.
- Real US DOT input files were not present in the inspected project. Acquisition,
  source terms, measured full-data volume, real monthly coverage, and real
  rankings/interpretation remain pending.
- Clean-machine installation commands are documented using verified Apache
  archive filenames, but were not executed on a fresh system.
- Literature records were verified; a full Pregel paper review is outstanding.
- Member names/contributions and the instructor's current rubric were not supplied.

Recovery commands are in README. Local synthetic success is not HDFS or real
dataset completion, and the report states that limitation.

## Completion work — 13 October 2026

- Downloaded Temurin JDK 11.0.32.1+1 and verified the publisher SHA-256. Installed
  it under the ignored project `.runtime` directory; Spark remains on JDK 17.
- Backed up the existing `hadoop-env.sh` and changed only its active Java path.
  Preserved the configured URI, NameNode/DataNode directories and existing data.
  No NameNode formatting, storage reset, JAR replacement or SSH-key change occurred.
- HDFS initially disappeared when its launching tool command exited. The first
  attempt `fixture-hdfs-final-001` is marked failed; a persistent terminal kept
  daemon processes alive for subsequent runs. Startup now also waits for safe
  mode to turn off before reporting readiness.
- `fixture-hdfs-final-002` completed HDFS ingestion, Spark read/write, cleaning,
  GraphX, persisted validation, independent graph tests, export and analysis.
  It passed 31 Scala saved-output checks, 57 Python export/oracle checks and all
  seven explicit graph cases. Eight Python unit/integrity tests passed after
  generating the fixture report with 13 named figures.
- Fixture raw and result paths were HEALTHY under HDFS fsck. Process inspection
  verified that both daemons used the installed JDK 11. The shared cluster
  reports 19 pre-existing under-replicated blocks; project paths use replication
  one and were verified separately. Unrelated stored data was preserved.
- Acquired Kaggle dataset 810/version 1 through its public endpoint. Archive
  integrity and exact file inventory passed; 5,819,079 flight records, 322 airport
  records and 14 airline records have no CSV width mismatches. See dataset provenance.
- Completed the research synthesis, tied-rank statistics, consolidated metrics,
  metadata coverage diagnostics, thirteen-figure set, source/run consistency
  checks and sanitized evidence packaging. Team requested equal contributions
  and editable Member 1, Member 2, Member 3 names.

## Real-data execution and finalization — 13–14 October 2026

`full-2015-final-001` completed on 13 October. It processed 5,819,079 raw flight
records, retained 5,231,130 (89.896%), and persisted 322 airports, 4,656 directed
routes and 2,346 undirected route pairs. All delay observations reconcile to
retained traffic; the delay sum is 25,558,794 minutes. No exact duplicate flight
records, unknown airline codes or invalid retained delay values were found.
Three active airports have missing coordinates; none have missing names.

All 31 saved-output checks and 16 full-data export/integrity checks passed.
Weighted and uniform-custom PageRank converged and conserved mass. The graph
has one weak component, 15,867 undirected triangles and mean local clustering
0.6554. Each of ATL, ORD and DFW is reachable from all 322 airports within three
legs. See the selected metrics for complete values and denominators.

Preprocessing took 139.319 materialized seconds (152 seconds including startup),
graph analysis 39.732 (52 including startup), validation 10.222 (21 including
startup), and the smoke check 2.988 (15 including startup). Spark used local[2],
a 2 GiB driver and four graph/shuffle partitions. Aggregation spill warnings
occurred without a failure. Peak memory was not instrumented; no peak-memory
or distributed speedup claim is made.

The overnight continuation on 14 October preserved the completed outputs and
restarted the persistent HDFS session. Full raw/result paths were checked with
HDFS fsck; daemon Java executables and file listings are in the selected
`hdfs-health.txt`. Thirteen real-data plots were reviewed; overlapping route
labels and inconsistent top margins were corrected before report generation.

October has 486,165 raw flights and zero retained flights. Of these, 482,878
reach the exclusive numeric-airport exclusion step; earlier cancellation/
diversion exclusions explain the difference. No complete-year claim is made.

## Repeatability and delivery checks — 14 October 2026

`full-2015-final-002` repeated the full HDFS pipeline with identical input hashes,
algorithm source and effective parameters. All 31 saved-output checks and 16
export/integrity checks passed again. `tests/compare_runs.py` verified matching
inventories and found all 14 non-timing exported tables equivalent within
absolute/relative tolerance 1e-8. This includes stable IDs, route aggregates,
centrality, connectivity, triangles, paths and cleaning audits.

The source/report ZIP was extracted into a temporary directory. All file hashes
and report image links verified; all eight unit/report tests passed from the
extracted package. The final archive adds the repeat-run manifest, validation
and stage timings alongside the comparison record. Raw data, runtime downloads,
local configuration, Hadoop backups and bulky logs are excluded.

The final guide marks executed acceptance checks and preserves detailed recovery
and reproduction steps. The prepared demo still needs the team's own rehearsal
and replacement of Member 1/2/3 with their names. A clean-machine installation
on a second computer and a multi-node benchmark were not performed. No updated
instructor rubric was supplied; the report follows the provided specification.

Repeat materialized / wrapper seconds:

| Stage | Materialized seconds | Wrapper seconds |
| --- | --- | --- |
| analytics | 40.887 | 55 |
| preprocess | 148.657 | 163 |
| smoke | 3.190 | 14 |
| validate | 10.523 | 22 |

The deliberate conflicting-airport lookup was re-run on 14 October and rejected
with nonzero status and no completion marker. `artifacts/failure-handling.json`
records that run. All shell scripts and 43 documented Bash snippets passed
syntax checks. No source code was published or pushed to a remote repository.

After all checks, the project-started DataNode and NameNode were stopped cleanly
with exit status zero, restoring the initial stopped-service state. Existing
and newly written HDFS data were retained. Start them again for the live demo.
