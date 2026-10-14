# Progress check-in 2 — 2026-10-13 — original fixture milestone

Historical snapshot: the status below describes the initial implementation.
See [execution_log.md](execution_log.md) for the subsequent HDFS and full-data execution.

All required GraphX algorithms execute on the fixture. Persisted-output
invariants and independent Python matrix oracles pass. A directed-chain test
verifies path direction; separate cases cover dangling/isolated vertices,
disconnected groups, reverse-ID triangles, and reciprocal edges. A conflicting
airport lookup fails with no completion marker.

The two fixture runs agree across 17 non-timing exported tables within 1e-8.
The first weighted run converged in 26 iterations with score sum 1.0. These are
synthetic correctness findings. HDFS integration and real airport findings are
not yet demonstrated.

## Completion update — 14 October 2026

The HDFS fixture and two full US DOT runs are now validated. The final network
contains 322 airports and 4,656 directed routes from 5,231,130 retained flights.
All 14 non-timing full-run tables repeat within tolerance 1e-8. Provenance,
monthly exclusions, real figures, complete report and portable packaging are
available. See the execution log for timings, tests, limitations and evidence.
