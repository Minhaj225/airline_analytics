# Literature survey and references

Sources were checked on 2026-10-13. Publisher abstracts and bibliographic
records were inspected, together with the Guimerà author preprint and the Pregel
paper hosted by UC Berkeley. Research findings motivate this design; they are
not measurements of this project.

| Research | Relevant contribution | Application and difference here |
| --- | --- | --- |
| Gonzalez et al., 2014 [1] | Combines graph abstractions with distributed dataflow | Spark ETL feeds GraphX after a persistence boundary; local fixtures do not reproduce the paper's distributed benchmarks |
| Zaharia et al., 2012 [2] | RDDs support reuse in iterative and interactive computation | Cached graph state and lineage/checkpoints support iteration; no worker-failure experiment was performed |
| Malewicz et al., 2010 [3] | Synchronous vertex programs exchange messages between supersteps; checkpointing supports recovery | GraphX supplies iterative primitives; this project uses aggregateMessages for weighted PageRank rather than a separate Pregel deployment |
| Guimerà et al., 2005 [4] | Air-network degree and centrality need not identify the same cities | Motivates comparing several metrics. Their worldwide, symmetrized city network differs from the directed US-airport baseline here; no community algorithm is implemented |
| Brin and Page, 1998 [5] | Uses hyperlink structure in a large search system | Link-based importance motivates the topology baseline; traffic counts supply this project's custom transition probabilities, not web relevance |

The practical contribution is the combination of a traceable cleaning policy,
explicit missing-delay denominators, independently checked weighted iteration,
and auditable exported results in a course-sized native pipeline. This is an
engineering reproduction and domain adaptation, not a new graph algorithm.
Synthetic fixtures establish software behavior. The selected real-data report
separately measures the airline network and describes the limits of one-machine
execution.

## Synthesis and design implications

GraphX [1] connects graph views to the distributed dataflow model. That connection
is useful here because flight cleaning is relational while centrality and
connectivity require repeated graph operations. Persisting and reloading graph
tables makes the boundary visible and independently testable. The original
paper's distributed performance results cannot be transferred to this laptop.

RDDs [2] motivate reusing immutable distributed state across iterative work.
This implementation materializes cached results before timing ends, releases
obsolete state, and periodically checkpoints long graph iterations. Those
choices do not amount to a worker-failure experiment; that evaluation would
require a separate cluster and controlled failures.

Pregel [3] organizes a graph program into synchronized supersteps: a vertex reads
prior messages and updates its state, with messages available in the next step.
Combiners reduce associative message traffic, while aggregators supply global
reductions. This project expresses weighted incoming rank through GraphX
aggregateMessages and dangling mass through a separate reduction. It does not
implement Pregel's C++ runtime, mutable topology, or cluster recovery protocol.
The full paper is available through a [university-hosted research copy](https://dsf.berkeley.edu/cs286/papers/pregel-sigmod2010.pdf).

Guimerà et al. [4] show why degree alone cannot substitute for all notions of
air-network importance. Their worldwide city-level analysis and centrality
measure differ from this directed US-airport PageRank analysis. The comparison
motivates measuring several metrics; it does not justify treating local triangles
or weak components as community detection, or asserting that the same rankings
should recur.

Brin and Page [5] use link structure as an importance signal in web search.
Here, the unweighted baseline treats distinct destinations uniformly, while the
custom transition probability reflects retained flight counts. Comparing weighted
and uniform custom iterations holds the implementation and convergence rule
fixed. The interpretation is circulation in a defined historical flight network,
not web relevance, passenger volume, or commercial importance.

## Research bibliography

1. Joseph E. Gonzalez, Reynold S. Xin, Ankur Dave, Daniel Crankshaw, Michael J.
   Franklin, and Ion Stoica. **GraphX: Graph Processing in a Distributed Dataflow
   Framework.** OSDI 2014, pp. 599–613.
   [Publisher](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/gonzalez).
2. Matei Zaharia, Mosharaf Chowdhury, Tathagata Das, Ankur Dave, Justin Ma,
   Murphy McCauley, Michael J. Franklin, Scott Shenker, and Ion Stoica.
   **Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory
   Cluster Computing.** NSDI 2012, pp. 15–28.
   [Publisher](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia).
3. Grzegorz Malewicz, Matthew H. Austern, Aart J. C. Bik, James C. Dehnert,
   Ilan Horn, Naty Leiser, and Grzegorz Czajkowski. **Pregel: a system for
   large-scale graph processing.** SIGMOD 2010, pp. 135–146.
   [Author organization](https://research.google/pubs/pregel-a-system-for-large-scale-graph-processing/).
4. Roger Guimerà, Stefano Mossa, Adrian Turtschi, and Luís A. N. Amaral.
   **The worldwide air transportation network: Anomalous centrality, community
   structure, and cities' global roles.** PNAS 102, 7794–7799 (2005).
   DOI [10.1073/pnas.0407994102](https://doi.org/10.1073/pnas.0407994102).
   [Author preprint](https://arxiv.org/abs/cond-mat/0312535).
5. Sergey Brin and Lawrence Page. **The Anatomy of a Large-Scale Hypertextual
   Web Search Engine.** Computer Networks 30 (1998), pp. 107–117.
   [Author organization](https://research.google/pubs/the-anatomy-of-a-large-scale-hypertextual-web-search-engine/).

The requested standalone PageRank technical-report server was inaccessible
during verification. Reference [5] is the verified alternative primary source;
the technical report's edition is not guessed or conflated with this article.

## Dataset and official documentation (not peer-reviewed research)

- [US DOT dataset on Kaggle](https://www.kaggle.com/datasets/usdot/flight-delays).
  The public metadata API identifies dataset 810, version 1, published by the
  Department of Transportation, licensed "CC0: Public Domain". The acquisition
  manifest records its response, download time, archive checksum, and CSV hashes.
  [Publisher metadata](https://www.kaggle.com/api/v1/datasets/view/usdot/flight-delays).
- [Spark 3.5.9 GraphX guide](https://spark.apache.org/docs/3.5.9/graphx-programming-guide.html).
  The guide documents graph construction and aggregateMessages. API behavior
  is additionally tested against the actual installed binaries by the fixtures.
- [Hadoop supported JVMs](https://cwiki.apache.org/confluence/spaces/HADOOP/pages/100827883/Hadoop%2BJava%2BVersions).
- [Spark archive](https://archive.apache.org/dist/spark/spark-3.5.9/) and
  [Hadoop archive](https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/).

The reference GitHub project named in the build plan was not used or adapted;
there is no consulted commit or inherited source-license claim to invent.
