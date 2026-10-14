# Data dictionary

Inputs are UTF-8 CSV with headers. All source columns are read as nullable
strings with an explicit schema based on the validated header; no numeric
inference is used for identifiers. Source files can contain extra columns.
Rows with a different field count receive `_corrupt_record`. CSV parsing is
single-line, matching the source dataset; multiline quoted flight records are
outside the input contract. Python manifest inspection reports width mismatches.

The baseline accepts valid 2015 calendar dates and flags literally `0` or `1`.
Only zero cancellation and diversion flags survive. Airport IDs are trimmed and
uppercased, then restricted to three ASCII letters. Lookup keys must be unique;
even identical duplicates fail to prevent accidental row multiplication.
Missing coordinates do not exclude airports. Unknown airline codes are counted
but do not remove flights. No numeric airport crosswalk is inferred.

| Table | Fields and meanings |
| --- | --- |
| clean/vertices | `vertex_id` Long: positive sorted IATA index; `iata` String: unique active airport; `airport_name`, `city`, `state`, `country` nullable Strings; `latitude`, `longitude` nullable Doubles, degrees |
| clean/edges | `src_id`, `dst_id` Long; `origin_iata`, `destination_iata` Strings; `num_flights` Long, positive; `delay_observations` Long; `sum_arrival_delay`, `avg_arrival_delay` nullable Double minutes; `delayed_flights_15min` Long, observations >=15 minutes |
| audit/cleaning_funnel | `step` Int, ordered cleaning priority; `reason` String; `records` Long assigned to exactly that reason; `remaining_records` Long after exclusion (retained row reports final total) |
| audit/exclusions_by_month | `YEAR`, `MONTH` nullable raw Strings, including invalid values; `raw_records`, `retained_records`, `numeric_excluded_records` Long. Numeric count is the exclusive funnel reason, not an overlapping diagnostic |
| audit/unmatched_airports | endpoint IATA pair and `count` Long, flights excluded at lookup stage |
| results/pagerank | `vertex_id`, `iata`; `score` raw GraphX Double; `normalized_score` Double summing to one; `rank` Int sorted by score descending and IATA ascending |
| results/weighted_pagerank | same ranking schema, custom flight-weighted scores with dangling redistribution |
| results/uniform_pagerank | same ranking schema, custom uniform-route-weight comparison |
| results/connected_components | `vertex_id`, `iata`, `component_id` Long, weak component label (not a numeric measure) |
| results/component_sizes | `component_id` Long; `airports` Long |
| results/triangle_count | `vertex_id`, `iata`; `triangles` Int per airport; `undirected_degree` Int; `clustering` Double in [0,1], zero if degree <2 |
| results/shortest_paths | `vertex_id`, `iata`, `landmark_id`, `landmark_iata`; `hops` nullable Int from vertex to landmark; `reachable` Boolean; null means unreachable, zero means landmark self-distance |
| results/airport_metrics | vertex metadata; `in_degree`, `out_degree`, `in_strength`, `out_strength` Long; `arrival_delay_observations`, `delayed_arrivals_15min` Long; nullable `arrival_delay_sum`, `avg_arrival_delay` Double minutes, weighted by observation counts |
| results/route_metrics | edge fields plus `delayed_fraction` nullable Double using delay observations as denominator |
| metrics/algorithm_timings | `algorithm` String; `materialize_seconds`, `write_seconds` Double wall seconds. Weighted iterative compute is recorded separately in each algorithm's JSON |

## Run-level JSON and analysis

`coverage.json` records the run ID, active airports with missing names or coordinate
pairs, and unique undirected edges. Missing coordinates do not remove airports.

`artifacts/<run_id>/metrics.json` consolidates the measured ETL counts, input bytes,
retained fraction, flights per directed route, average-tie-rank Spearman ranking
comparisons, degree/traffic summaries, component membership, triangle/clustering
statistics, hub hop distributions, monthly coverage, delay observation denominators,
qualifying route delays, and stage timings. Correlation is null when undefined
(too few usable airports or constant values). Airport delay correlations give each
usable airport equal weight; the overall flight-delay mean weights individual
observations. These are different estimands.

`findings.md` is generated from that selected-run metrics object. It labels fixture
results explicitly and does not infer causality. `analysis.json` retains an older
IATA-tie-broken rank diagnostic under an explicit name; report comparisons use
the average-tied-rank statistic in `metrics.json` instead.

`run_manifest.json` version 2 embeds the immutable dataset manifest, effective
configuration, completed stages, input provenance and computation source hashes.
Wall times overlap materialized stage durations and must not be added together.
The final HDFS health record separately verifies live service state, storage
placement and daemon Java executables.

Parquet preserves types and nulls. CSV uses empty cells for null and lowercase
`true`/`false`. Delay can be negative (early arrival); missing, unparseable, NaN,
and infinite values contribute no delay observation. All-null route delays
have null sum and mean. Traffic counts still include these flights.

Files are Spark directories, not individual CSV files. The exporter reads every
part file with a CSV parser, checks matching headers, and emits one header.
Physical row order is unspecified. Use `rank`, `step`, airport codes, or endpoint
IDs for semantic ordering; never rely on part filename order as ranking order.

The JSON manifests record effective configuration, input byte hashes and record
counts, mode, status, stages, and measured versions. `validation.json` records
persisted Parquet checks; `analysis.json` records export integrity and independent
fixture oracles. A run becomes `validated` only after both pass.
