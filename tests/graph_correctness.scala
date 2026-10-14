object GraphCorrectness {
  def run(spark: SparkSession, c: ProjectConfig): Unit = {
    import spark.implicits._
    val sc = spark.sparkContext
    def graph(ids: Seq[Long], edges: Seq[(Long, Long, Double)]): Graph[String, Double] =
      Graph(sc.parallelize(ids.map(i => (i, i.toString))),
        sc.parallelize(edges.map { case (s, d, w) => Edge(s, d, w) }), "")
    val cycle = graph(Seq(1L, 2L, 3L), Seq((3L, 2L, 1.0), (2L, 1L, 1.0), (1L, 3L, 1.0)))
    val equal = WeightedPageRank.run(cycle, 0.15, 1e-10, 200)
    require(equal.graph.vertices.collect().forall(x => math.abs(x._2 - 1.0 / 3) < 1e-9), "Cycle must have equal rank")
    val triangles = TriangleCount.runPreCanonicalized(GraphAnalytics.projection(cycle, 2))
    require(triangles.vertices.collect().forall(_._2 == 1), "Reverse-ID triangle must survive projection")
    require(cycle.connectedComponents().vertices.map(_._2).distinct().count() == 1)
    val chain = graph(Seq(1L, 2L, 3L, 4L, 5L, 6L), Seq((1L, 2L, 1.0), (2L, 3L, 1.0), (5L, 6L, 1.0)))
    val paths = ShortestPaths.run(chain, Seq(1L, 3L)).vertices.collect().toMap
    require(paths(1L)(3L) == 2 && paths(2L)(3L) == 1 && paths(3L)(3L) == 0)
    require(!paths(2L).contains(1L) && !paths(3L).contains(1L) && paths(4L).isEmpty, "Paths must go TO landmarks")
    require(chain.connectedComponents().vertices.map(_._2).distinct().count() == 3)
    val chainRank = WeightedPageRank.run(chain, 0.15, 1e-10, 200)
    require(chainRank.graph.vertices.count() == 6 && math.abs(chainRank.mass - 1.0) < 1e-9)
    val asymmetric = graph(Seq(1L, 2L, 3L, 4L, 5L, 6L),
      Seq((1L, 2L, 9.0), (1L, 3L, 1.0), (2L, 1L, 1.0), (3L, 2L, 1.0), (5L, 6L, 1.0)))
    val weighted = WeightedPageRank.run(asymmetric, 0.15, 1e-10, 200)
    val uniform = WeightedPageRank.run(asymmetric.mapEdges(_ => 1.0), 0.15, 1e-10, 200)
    require(weighted.converged && uniform.converged)
    val difference = weighted.graph.vertices.join(uniform.graph.vertices).map { case (_, (a, b)) => math.abs(a - b) }.max()
    require(difference > 0.01, "Asymmetric weights must affect rank")
    val reciprocal = graph(Seq(1L, 2L), Seq((1L, 2L, 1.0), (2L, 1L, 1.0)))
    require(GraphAnalytics.projection(reciprocal, 2).edges.count() == 1)
    Output.table(weighted.graph.vertices.toDF("vertex_id", "score"), c, "tests/asymmetric_weighted")
    Output.table(uniform.graph.vertices.toDF("vertex_id", "score"), c, "tests/asymmetric_uniform")
    Output.table(chainRank.graph.vertices.toDF("vertex_id", "score"), c, "tests/chain_rank")
    Output.json(spark, s"${c.run}/tests/graph_correctness.json",
      s"""{"run_id":"${c.runId}","passed":true,"cases":["equal_cycle","reverse_triangle","directed_chain","disconnected_groups","dangling_and_isolated","weight_asymmetry","reciprocal_projection"]}""")
    Seq(cycle, chain, asymmetric, reciprocal).foreach(_.unpersist())
    Seq(equal, chainRank, weighted, uniform).foreach(_.graph.unpersist())
  }
}
