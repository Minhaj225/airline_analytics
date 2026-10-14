# Real-data findings

## Selected-run findings: full-2015-final-001

**Measured retained-2015-flight findings.**

### Centrality and traffic

The five leading traffic-weighted airports are ATL (0.061534), ORD (0.052170), DFW (0.044481), DEN (0.035022), LAX (0.029258). Weighted versus uniform-custom PageRank has Spearman correlation 0.9121, with 18 shared airports in their top 20. This comparison holds the iterative algorithm and stopping parameters fixed; flight-count transition probabilities are the changed input.

The largest absolute rank change is HDN: uniform rank 102 versus traffic-weighted rank 273. Route count and repeated traffic therefore need not assign the same importance.

Across active airports, weighted-rank association with incoming traffic is 0.9526673780462214; with distinct incoming routes it is 0.8859064900281126 (Spearman, average tied ranks).

### Connectivity and neighborhood structure

The graph contains 1 weak component(s); the largest contains 100.0% of active airports. The undirected projection contains 15,867 triangles, and the mean per-airport local clustering coefficient is 0.6554. A component measures weak connectivity, whereas triangles measure local neighbor interconnection; neither result is a community partition. A clustering coefficient of one can occur at low-degree airports and must be read alongside degree and raw triangle count.

| Landmark | Reachable airports | Unreachable | Maximum legs |
| --- | --- | --- | --- |
| ATL | 322 | 0 | 3 |
| ORD | 322 | 0 | 3 |
| DFW | 322 | 0 | 3 |

### Delay denominators and scope

Arrival delay is observed for 5,231,130 of 5,231,130 retained flights (100.00%); the observation-weighted mean is 4.886 minutes. At the 100-flight threshold, 4,075 routes have an observed average. Airport weighted-centrality versus mean delay uses 322 airports; Spearman correlation (average tied ranks) is 0.0646. This is descriptive association; airport mix, season, weather and scheduling are uncontrolled. It does not establish delay propagation.

The largest exclusive numeric-ID exclusion count occurs in 2015/10: 482,878 records, with 0 of 486,165 monthly records retained. The graph represents retained records, so it must not be described as complete-year service coverage.

All figures and tables derive from this run. Times are measured on one machine; the final graph is much smaller than the raw flight table.

## Relation to the research questions

The ranking disagreement supports measuring traffic separately from distinct
route connectivity. This follows the motivation of Guimerà et al. to distinguish
notions of importance, but their worldwide city graph and betweenness measure
differ from this US-airport PageRank experiment. The findings do not reproduce
that paper's numerical results or community assignments.

The pipeline follows the GraphX integration model described by Gonzalez et al.:
relational input processing produces graph state for iterative analysis.
139.32 seconds of materialized preprocessing and 39.73 seconds of graph analysis
were measured in the selected run; their wrapper times were 152 and 52 seconds.
These are one-machine observations, with process startup reported separately,
and do not replicate the research paper's distributed benchmarks.

All delay means describe retained completed flights. October's absence,
cancellations/diversions, geographic coverage and aggregation over time limit
interpretation. An airport-level correlation of about 0.065 provides little
monotonic association in this descriptive summary; it is not a significance
test or causal result. Full per-route denominators and monthly counts are
included in the report.

See [literature survey](literature_survey.md) for the primary research sources
and [dataset provenance](dataset_provenance.md) for acquired bytes and hashes.
