window.DASHBOARD_DATA = {
  "generated_at": "2026-10-15T07:08:09.669337+00:00",
  "run": {
    "id": "full-2015-final-001",
    "mode": "full",
    "storage": "hdfs",
    "dataset_id": "usdot-2015-v1",
    "status": "validated",
    "spark_master": "local[2]",
    "graph_partitions": 4,
    "started_at": "2026-10-13T16:58:15.324460+00:00"
  },
  "summary": {
    "raw_records": 5819079,
    "retained_records": 5231130,
    "retained_fraction": 0.8989618460240874,
    "airports": 322,
    "routes": 4656,
    "input_bytes": 592430817,
    "triangles": 15867,
    "weak_components": 1,
    "mean_arrival_delay": 4.885903045804635,
    "delay_correlation": 0.06460501331654088
  },
  "cleaning": [
    {
      "step": 1,
      "reason": "corrupt",
      "records": 0,
      "remaining": 5819079
    },
    {
      "step": 2,
      "reason": "invalid date or flags",
      "records": 0,
      "remaining": 5819079
    },
    {
      "step": 3,
      "reason": "cancelled or diverted",
      "records": 105071,
      "remaining": 5714008
    },
    {
      "step": 4,
      "reason": "numeric airport",
      "records": 482878,
      "remaining": 5231130
    },
    {
      "step": 5,
      "reason": "malformed airport",
      "records": 0,
      "remaining": 5231130
    },
    {
      "step": 6,
      "reason": "self loop",
      "records": 0,
      "remaining": 5231130
    },
    {
      "step": 7,
      "reason": "unmatched airport",
      "records": 0,
      "remaining": 5231130
    },
    {
      "step": 8,
      "reason": "retained",
      "records": 5231130,
      "remaining": 5231130
    }
  ],
  "monthly": [
    {
      "year": "2015",
      "month": "1",
      "raw_records": 469968,
      "retained_records": 457013,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9724342933986995
    },
    {
      "year": "2015",
      "month": "2",
      "raw_records": 429191,
      "retained_records": 407663,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9498405138970761
    },
    {
      "year": "2015",
      "month": "3",
      "raw_records": 504312,
      "retained_records": 492138,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9758601817922239
    },
    {
      "year": "2015",
      "month": "4",
      "raw_records": 485151,
      "retained_records": 479251,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9878388378051369
    },
    {
      "year": "2015",
      "month": "5",
      "raw_records": 496993,
      "retained_records": 489641,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9852070351091464
    },
    {
      "year": "2015",
      "month": "6",
      "raw_records": 503897,
      "retained_records": 492847,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9780709152862589
    },
    {
      "year": "2015",
      "month": "7",
      "raw_records": 520718,
      "retained_records": 514384,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9878360264096882
    },
    {
      "year": "2015",
      "month": "8",
      "raw_records": 510536,
      "retained_records": 503956,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9871115846874657
    },
    {
      "year": "2015",
      "month": "9",
      "raw_records": 464946,
      "retained_records": 462153,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9939928507826715
    },
    {
      "year": "2015",
      "month": "10",
      "raw_records": 486165,
      "retained_records": 0,
      "numeric_excluded_records": 482878,
      "retained_fraction": 0.0
    },
    {
      "year": "2015",
      "month": "11",
      "raw_records": 467972,
      "retained_records": 462367,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.9880227876881523
    },
    {
      "year": "2015",
      "month": "12",
      "raw_records": 479230,
      "retained_records": 469717,
      "numeric_excluded_records": 0,
      "retained_fraction": 0.980149406339336
    }
  ],
  "top_airports": [
    {
      "rank": 1,
      "iata": "ATL",
      "name": "Hartsfield-Jackson Atlanta International Airport",
      "city": "Atlanta",
      "score": 0.06153379270066174,
      "incoming_flights": 343076,
      "outgoing_flights": 343506,
      "avg_arrival_delay": 2.2262618195385278
    },
    {
      "rank": 2,
      "iata": "ORD",
      "name": "Chicago O'Hare International Airport",
      "city": "Chicago",
      "score": 0.052170269661982197,
      "incoming_flights": 275864,
      "outgoing_flights": 276554,
      "avg_arrival_delay": 7.240803439375925
    },
    {
      "rank": 3,
      "iata": "DFW",
      "name": "Dallas/Fort Worth International Airport",
      "city": "Dallas-Fort Worth",
      "score": 0.04448103891596952,
      "incoming_flights": 231764,
      "outgoing_flights": 232647,
      "avg_arrival_delay": 5.866243247441362
    },
    {
      "rank": 4,
      "iata": "DEN",
      "name": "Denver International Airport",
      "city": "Denver",
      "score": 0.03502151722819939,
      "incoming_flights": 193033,
      "outgoing_flights": 193402,
      "avg_arrival_delay": 5.082923645179839
    },
    {
      "rank": 5,
      "iata": "LAX",
      "name": "Los Angeles International Airport",
      "city": "Los Angeles",
      "score": 0.029257640782644003,
      "incoming_flights": 192136,
      "outgoing_flights": 192003,
      "avg_arrival_delay": 6.109568222509056
    },
    {
      "rank": 6,
      "iata": "IAH",
      "name": "George Bush Intercontinental Airport",
      "city": "Houston",
      "score": 0.024679367049484067,
      "incoming_flights": 143587,
      "outgoing_flights": 144019,
      "avg_arrival_delay": 5.651242800532081
    },
    {
      "rank": 7,
      "iata": "SFO",
      "name": "San Francisco International Airport",
      "city": "San Francisco",
      "score": 0.024349537328576146,
      "incoming_flights": 145409,
      "outgoing_flights": 145491,
      "avg_arrival_delay": 6.581958475747719
    },
    {
      "rank": 8,
      "iata": "PHX",
      "name": "Phoenix Sky Harbor International Airport",
      "city": "Phoenix",
      "score": 0.022659876404932514,
      "incoming_flights": 145378,
      "outgoing_flights": 145552,
      "avg_arrival_delay": 2.912139388353121
    },
    {
      "rank": 9,
      "iata": "MSP",
      "name": "Minneapolis-Saint Paul International Airport",
      "city": "Minneapolis",
      "score": 0.02213253540541114,
      "incoming_flights": 111146,
      "outgoing_flights": 111055,
      "avg_arrival_delay": 3.3743634498767388
    },
    {
      "rank": 10,
      "iata": "SLC",
      "name": "Salt Lake City International Airport",
      "city": "Salt Lake City",
      "score": 0.021080684427854907,
      "incoming_flights": 96557,
      "outgoing_flights": 96505,
      "avg_arrival_delay": 0.39525875907495056
    }
  ],
  "reachability": {
    "ATL": {
      "reachable": 322,
      "unreachable": 0,
      "maximum_hops": 3,
      "hop_distribution": {
        "0": 1,
        "1": 168,
        "2": 149,
        "3": 4
      },
      "unreachable_airports": []
    },
    "ORD": {
      "reachable": 322,
      "unreachable": 0,
      "maximum_hops": 3,
      "hop_distribution": {
        "0": 1,
        "1": 162,
        "2": 155,
        "3": 4
      },
      "unreachable_airports": []
    },
    "DFW": {
      "reachable": 322,
      "unreachable": 0,
      "maximum_hops": 3,
      "hop_distribution": {
        "0": 1,
        "1": 149,
        "2": 168,
        "3": 4
      },
      "unreachable_airports": []
    }
  },
  "rank_comparison": {
    "spearman": 0.9120620882836801,
    "top_n": 20,
    "overlap": 18
  },
  "delay": {
    "coverage": 1.0,
    "threshold": 100,
    "qualifying_routes": 4075,
    "highest_routes": [
      {
        "origin": "ASE",
        "destination": "DFW",
        "flights": 262,
        "avg_delay": 39.965648854961835,
        "delayed_fraction": 0.42748091603053434
      },
      {
        "origin": "JFK",
        "destination": "EGE",
        "flights": 105,
        "avg_delay": 38.00952380952381,
        "delayed_fraction": 0.3142857142857143
      },
      {
        "origin": "OAK",
        "destination": "IAH",
        "flights": 174,
        "avg_delay": 34.02873563218391,
        "delayed_fraction": 0.5172413793103449
      },
      {
        "origin": "ACY",
        "destination": "DTW",
        "flights": 145,
        "avg_delay": 30.896551724137932,
        "delayed_fraction": 0.41379310344827586
      },
      {
        "origin": "MSP",
        "destination": "TTN",
        "flights": 151,
        "avg_delay": 30.225165562913908,
        "delayed_fraction": 0.44370860927152317
      },
      {
        "origin": "SRQ",
        "destination": "LGA",
        "flights": 420,
        "avg_delay": 30.169047619047618,
        "delayed_fraction": 0.38571428571428573
      },
      {
        "origin": "CRW",
        "destination": "DFW",
        "flights": 138,
        "avg_delay": 29.615942028985508,
        "delayed_fraction": 0.4057971014492754
      },
      {
        "origin": "ACY",
        "destination": "BOS",
        "flights": 180,
        "avg_delay": 29.42222222222222,
        "delayed_fraction": 0.42777777777777776
      }
    ]
  },
  "stages": [
    {
      "name": "analytics",
      "status": "complete",
      "wall_seconds": 52,
      "materialized_seconds": 39.732363202
    },
    {
      "name": "preprocess",
      "status": "complete",
      "wall_seconds": 152,
      "materialized_seconds": 139.319272724
    },
    {
      "name": "smoke",
      "status": "complete",
      "wall_seconds": 15,
      "materialized_seconds": 2.988209956
    },
    {
      "name": "validate",
      "status": "complete",
      "wall_seconds": 21,
      "materialized_seconds": 10.221631826
    }
  ],
  "algorithms": [
    {
      "name": "pagerank",
      "compute_seconds": 2.647904391,
      "write_seconds": 0.951851523
    },
    {
      "name": "weighted pagerank",
      "compute_seconds": 0.233251322,
      "write_seconds": 0.972805794
    },
    {
      "name": "uniform pagerank",
      "compute_seconds": 0.191907187,
      "write_seconds": 0.960845763
    },
    {
      "name": "connected components",
      "compute_seconds": 0.371682884,
      "write_seconds": 0.9609278
    },
    {
      "name": "component sizes",
      "compute_seconds": 0.292375258,
      "write_seconds": 0.97368337
    },
    {
      "name": "triangle count",
      "compute_seconds": 0.423355517,
      "write_seconds": 1.849598332
    },
    {
      "name": "shortest paths",
      "compute_seconds": 0.387191399,
      "write_seconds": 0.957208951
    },
    {
      "name": "airport metrics",
      "compute_seconds": 0.38374991,
      "write_seconds": 0.991499501
    },
    {
      "name": "route metrics",
      "compute_seconds": 0.083122348,
      "write_seconds": 0.606711128
    }
  ],
  "validation": {
    "passed": true,
    "saved_output_checks": 31,
    "export_checks": 16,
    "checks": [
      {
        "check": "nonempty_graph",
        "passed": true
      },
      {
        "check": "unique_vertices",
        "passed": true
      },
      {
        "check": "stable_sorted_ids",
        "passed": true
      },
      {
        "check": "endpoints_exist",
        "passed": true
      },
      {
        "check": "simple_directed_routes",
        "passed": true
      },
      {
        "check": "flight_conservation",
        "passed": true
      },
      {
        "check": "delay_count_conservation",
        "passed": true
      },
      {
        "check": "delay_sum_conservation",
        "passed": true
      },
      {
        "check": "route_denominators",
        "passed": true
      },
      {
        "check": "cleaning_funnel",
        "passed": true
      },
      {
        "check": "pagerank_finite_nonnegative",
        "passed": true
      },
      {
        "check": "pagerank_labels",
        "passed": true
      },
      {
        "check": "weighted_pagerank_finite_nonnegative",
        "passed": true
      },
      {
        "check": "weighted_pagerank_labels",
        "passed": true
      },
      {
        "check": "weighted_pagerank_mass",
        "passed": true
      },
      {
        "check": "weighted_pagerank_converged",
        "passed": true
      },
      {
        "check": "uniform_pagerank_finite_nonnegative",
        "passed": true
      },
      {
        "check": "uniform_pagerank_labels",
        "passed": true
      },
      {
        "check": "uniform_pagerank_mass",
        "passed": true
      },
      {
        "check": "uniform_pagerank_converged",
        "passed": true
      },
      {
        "check": "component_membership",
        "passed": true
      },
      {
        "check": "triangle_divisibility",
        "passed": true
      },
      {
        "check": "clustering_bounds",
        "passed": true
      },
      {
        "check": "path_grid",
        "passed": true
      },
      {
        "check": "path_null_semantics",
        "passed": true
      },
      {
        "check": "landmark_self_distance",
        "passed": true
      },
      {
        "check": "airport_traffic_conservation",
        "passed": true
      },
      {
        "check": "airport_delay_conservation",
        "passed": true
      },
      {
        "check": "route_export_matches_clean",
        "passed": true
      },
      {
        "check": "monthly_conservation",
        "passed": true
      },
      {
        "check": "component_sizes_match_membership",
        "passed": true
      }
    ]
  },
  "figures": [
    {
      "title": "Cleaning decisions and retained records",
      "src": "../report/figures/full-2015-final-001/cleaning.png"
    },
    {
      "title": "Monthly retention and numeric-ID exclusions",
      "src": "../report/figures/full-2015-final-001/monthly.png"
    },
    {
      "title": "Topology, traffic-weighted, and uniform PageRank",
      "src": "../report/figures/full-2015-final-001/pagerank.png"
    },
    {
      "title": "Incoming traffic and weighted centrality",
      "src": "../report/figures/full-2015-final-001/strength.png"
    },
    {
      "title": "Distribution of distinct route connections",
      "src": "../report/figures/full-2015-final-001/degree_distribution.png"
    },
    {
      "title": "Distribution of airport flight traffic",
      "src": "../report/figures/full-2015-final-001/traffic_distribution.png"
    },
    {
      "title": "Local clustering and weak connectivity",
      "src": "../report/figures/full-2015-final-001/clustering.png"
    },
    {
      "title": "Weak component size distribution",
      "src": "../report/figures/full-2015-final-001/component_sizes.png"
    },
    {
      "title": "Airports with the most undirected triangles",
      "src": "../report/figures/full-2015-final-001/triangles.png"
    },
    {
      "title": "Directed flight legs TO each landmark",
      "src": "../report/figures/full-2015-final-001/reachability.png"
    },
    {
      "title": "Route delays: at least 100 flights",
      "src": "../report/figures/full-2015-final-001/delay.png"
    },
    {
      "title": "Airport centrality and observed arrival delay",
      "src": "../report/figures/full-2015-final-001/delay_centrality.png"
    },
    {
      "title": "Measured pipeline durations",
      "src": "../report/figures/full-2015-final-001/timings.png"
    }
  ],
  "limitations": [
    "October has zero retained rows because numeric airport identifiers are excluded.",
    "Weak components are connectivity groups, not detected communities.",
    "Delay results are descriptive associations and do not establish causal propagation.",
    "The measured timings come from one local machine and do not prove cluster speedup.",
    "The aggregated airport graph is much smaller than the raw flight table."
  ]
};
