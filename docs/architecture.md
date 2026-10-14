# Architecture and implemented boundary

```text
flights.csv + airports.csv + airlines.csv
                |
       CSV inspection + SHA-256 manifest
                |
      HDFS raw/<dataset_id> (default)
                |
     Scala Spark SQL: parse, audit, filter, aggregate
                |
     runs/<run_id>/clean/{vertices,edges} Parquet
                |
             Parquet reload
                |
       Scala GraphX, EdgePartition2D
       | topology PageRank / weighted & uniform PageRank
       | weak components / undirected triangles
       | directed distances TO ATL, ORD, DFW
                |
     persisted results -> reload validation
                |
       multi-part CSV export + checksums
                |
       Python aggregate checks + fixture matrix oracle
```

`--storage local --mode fixture` exercises the same Scala stages using `file://`
and is a development fallback. It does not demonstrate HDFS ingestion, HDFS
durability, or distributed performance. Full mode requires HDFS. Spark runs with
`local[2]` by default; no YARN or standalone Spark master is required.

The preprocessing stage keeps repeated flight records as traffic weights,
audits exact duplicates separately, and never collects raw flights. Airport
mapping is limited to 10,000 entries. Every run uses immutable output paths.
Partial runs remain available for diagnosis and cannot be silently overwritten.

The weighted transition probability is `num_flights / outgoing_flights`.
Scores start at `1/N`; each iteration adds `alpha/N` and distributes dangling
mass uniformly. The stopping rule is the maximum absolute score change. A
separate uniform-weight run uses exactly the same implementation. The built-in
GraphX PageRank baseline is compared separately because its normalization and
dangling handling differ. Its score is also exported divided by the score sum.

GraphX computes on a directed graph. Triangles use a separate deduplicated
undirected graph with `src < dst`; the original direction never determines
whether an undirected edge survives. Weak components are not communities.
Landmark distances count flight legs toward hubs; they are not itineraries.

The default replication factor one on a single machine offers no physical-node
redundancy. Millions of flights would reduce to a relatively small airport graph;
the project demonstrates a scalable processing design, not a claim that the
aggregated graph requires a cluster.
