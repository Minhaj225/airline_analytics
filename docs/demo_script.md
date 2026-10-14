# Demonstration — at most ten minutes

Use selected run `full-2015-final-001`. Its results are **precomputed real-data
results**. The separate fixture is synthetic correctness evidence. Rehearse
this sequence before presenting; a live full run is unnecessary.

| Time | Show and explain |
| --- | --- |
| 0:00–1:00 | Architecture and tool roles: HDFS storage, Spark SQL cleaning, Scala GraphX analysis |
| 1:00–3:00 | Dataset provenance: 5,819,079 flights; HDFS raw listing and healthy selected-run paths |
| 3:00–5:00 | 5,231,130 retained flights become 322 airports/4,656 directed routes; October numeric-ID exclusions remove that month's coverage |
| 5:00–8:00 | Show PageRank, component/triangle tables and hub-hop figure; 31 saved checks, fixture matrix oracle, directed-chain and deliberate failure tests |
| 8:00–10:00 | ATL/ORD/DFW lead weighted centrality; all airports reach each hub within three legs; weak centrality-delay association; show report/package and limitations |

Start HDFS in a persistent terminal before a live storage demonstration:

```bash
bash scripts/start_hdfs.sh
hdfs dfs -ls -h "/user/${USER}/airline_graph/raw/usdot-2015-v1"
hdfs dfs -ls "/user/${USER}/airline_graph/runs/full-2015-final-001/results"
python3 src/python/analyze_results.py --run-id full-2015-final-001
```

Use the saved `hdfs-health.txt` when live services are unavailable, and say so.
Read the real monthly chart: October has zero retained rows. Do not claim a
complete-year graph, causal delay propagation or measured distributed speedup.

Prepare answers about separate Java versions, dangling PageRank mass,
weight-controlled comparisons, numeric-ID bias, weak components versus
communities, triangle canonicalization, shortest-path direction, missing
versus negative delays, observation denominators and materialized timings.
Replace the three editable member names in the contribution statement.
