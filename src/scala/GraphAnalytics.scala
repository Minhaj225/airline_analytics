import org.apache.spark.graphx.lib.{TriangleCount, ShortestPaths}

object GraphAnalytics {
  def projection(graph: Graph[String, Double], partitions: Int): Graph[String, Int] = {
    val pairs = graph.edges.filter(e => e.srcId != e.dstId)
      .map(e => (math.min(e.srcId, e.dstId), math.max(e.srcId, e.dstId))).distinct()
    Graph(graph.vertices, pairs.map { case (a, b) => Edge(a, b, 1) }, "")
      .partitionBy(PartitionStrategy.EdgePartition2D, partitions)
  }
  def run(spark: SparkSession, c: ProjectConfig): Unit = {
    import spark.implicits._
    val vertices = spark.read.parquet(s"${c.run}/clean/vertices").cache()
    val edges = spark.read.parquet(s"${c.run}/clean/edges").cache()
    require(edges.join(vertices, edges("src_id") === vertices("vertex_id"), "left_anti").count() == 0 &&
      edges.join(vertices, edges("dst_id") === vertices("vertex_id"), "left_anti").count() == 0, "Unknown edge endpoint")
    val graph = Graph(vertices.select("vertex_id", "iata").as[(Long, String)].rdd,
      edges.select("src_id", "dst_id", "num_flights").as[(Long, Long, Long)].rdd
        .map { case (src, dst, flights) => Edge(src, dst, flights.toDouble) }, "")
      .partitionBy(PartitionStrategy.EdgePartition2D, c.graphPartitions).cache()
    graph.vertices.count(); graph.edges.count()
    val timings = scala.collection.mutable.ArrayBuffer.empty[(String, Double, Double)]
    def save(name: String)(compute: => DataFrame): Unit = {
      val start = System.nanoTime()
      val result = compute.cache()
      result.count()
      val computed = System.nanoTime()
      Output.table(result, c, s"results/$name")
      timings += ((name, (computed - start) / 1e9, (System.nanoTime() - computed) / 1e9))
      result.unpersist()
    }
    def ranks(scores: DataFrame): DataFrame = {
      val total = scores.agg(sum("score")).first().getDouble(0)
      scores.join(vertices.select("vertex_id", "iata"), Seq("vertex_id"))
        .withColumn("normalized_score", col("score") / total)
        .withColumn("rank", row_number().over(Window.orderBy(col("score").desc, col("iata"))))
    }
    save("pagerank") {
      val result = graph.staticPageRank(c.baselineIterations, c.alpha)
      ranks(result.vertices.toDF("vertex_id", "score"))
    }
    for ((name, input) <- Seq("weighted_pagerank" -> graph, "uniform_pagerank" -> graph.mapEdges(_ => 1.0))) {
      val start = System.nanoTime()
      val result = WeightedPageRank.run(input, c.alpha, c.tolerance, c.maxIterations)
      val seconds = (System.nanoTime() - start) / 1e9
      save(name) { ranks(result.graph.vertices.toDF("vertex_id", "score")) }
      Output.json(spark, s"${c.run}/metrics/$name.json",
        s"""{"run_id":"${c.runId}","iterations":${result.iterations},"residual":${result.residual},"converged":${result.converged},"score_sum":${result.mass},"compute_seconds":$seconds}""")
      result.graph.unpersistVertices(false)
    }
    save("connected_components") {
      graph.connectedComponents().vertices.toDF("vertex_id", "component_id")
        .join(vertices.select("vertex_id", "iata"), Seq("vertex_id"))
    }
    save("component_sizes") {
      spark.read.parquet(s"${c.run}/results/connected_components").groupBy("component_id")
        .agg(count(lit(1)).as("airports"))
    }
    val undirected = projection(graph, c.graphPartitions).cache()
    save("triangle_count") {
      val triangles = TriangleCount.runPreCanonicalized(undirected).vertices.toDF("vertex_id", "triangles")
      vertices.select("vertex_id", "iata").join(triangles, Seq("vertex_id"), "left")
        .join(undirected.degrees.toDF("vertex_id", "undirected_degree"), Seq("vertex_id"), "left")
        .na.fill(0, Seq("triangles", "undirected_degree"))
        .withColumn("clustering", when(col("undirected_degree") < 2, 0.0)
          .otherwise(col("triangles") * 2.0 / (col("undirected_degree").cast("double") * (col("undirected_degree") - 1))))
    }
    val labels = vertices.select("vertex_id", "iata").as[(Long, String)].collect().toMap
    val landmarks = c.landmarks.map(code => labels.find(_._2 == code).map(_._1)
      .getOrElse(throw new IllegalArgumentException(s"Required landmark missing: $code")))
    save("shortest_paths") {
      ShortestPaths.run(graph, landmarks).vertices.flatMap { case (id, distances) =>
        landmarks.map(l => (id, l, distances.get(l), distances.contains(l)))
      }.toDF("vertex_id", "landmark_id", "hops", "reachable")
        .join(vertices.select("vertex_id", "iata"), Seq("vertex_id"))
        .join(vertices.select(col("vertex_id").as("landmark_id"), col("iata").as("landmark_iata")), Seq("landmark_id"))
    }
    save("airport_metrics") {
      val incoming = edges.groupBy(col("dst_id").as("vertex_id")).agg(count(lit(1)).as("in_degree"),
        sum("num_flights").as("in_strength"), sum("delay_observations").as("arrival_delay_observations"),
        sum("sum_arrival_delay").as("arrival_delay_sum"), sum("delayed_flights_15min").as("delayed_arrivals_15min"))
      val outgoing = edges.groupBy(col("src_id").as("vertex_id")).agg(count(lit(1)).as("out_degree"), sum("num_flights").as("out_strength"))
      vertices.join(incoming, Seq("vertex_id"), "left").join(outgoing, Seq("vertex_id"), "left")
        .na.fill(0L, Seq("in_degree", "in_strength", "out_degree", "out_strength", "arrival_delay_observations", "delayed_arrivals_15min"))
        .withColumn("avg_arrival_delay", when(col("arrival_delay_observations") > 0, col("arrival_delay_sum") / col("arrival_delay_observations")))
    }
    save("route_metrics") { edges.withColumn("delayed_fraction", when(col("delay_observations") > 0, col("delayed_flights_15min") / col("delay_observations"))) }
    Output.table(timings.toSeq.toDF("algorithm", "materialize_seconds", "write_seconds"), c, "metrics/algorithm_timings")
    undirected.unpersist(); graph.unpersist(); vertices.unpersist(); edges.unpersist()
  }
}
