object Validation {
  def run(spark: SparkSession, c: ProjectConfig): Unit = {
    import spark.implicits._
    val checks = scala.collection.mutable.ArrayBuffer.empty[(String, Boolean, String)]
    def check(name: String, passed: Boolean, observed: Any): Unit = checks += ((name, passed, observed.toString))
    def read(path: String): DataFrame = spark.read.parquet(s"${c.run}/$path")
    def total(df: DataFrame, column: String): Double = Option(df.agg(sum(column)).first().get(0)).map(_.toString.toDouble).getOrElse(0.0)
    val v = read("clean/vertices"); val e = read("clean/edges")
    val n = v.count(); val m = e.count()
    val etl = spark.read.json(Seq(Output.readJson(spark, s"${c.run}/metrics/etl.json")).toDS()).first()
    check("nonempty_graph", n > 0 && m > 0, s"vertices=$n edges=$m")
    check("unique_vertices", v.select("vertex_id").distinct().count() == n && v.select("iata").distinct().count() == n, n)
    val mapping = v.select("vertex_id", "iata").as[(Long, String)].collect().sortBy(_._2)
    check("stable_sorted_ids", mapping.map(_._1).sameElements((1L to n).toArray[Long]), mapping.mkString(","))
    check("endpoints_exist", Seq("src_id", "dst_id").forall(k => e.join(v, e(k) === v("vertex_id"), "left_anti").count() == 0), m)
    check("simple_directed_routes", e.filter(col("src_id") === col("dst_id")).count() == 0 && e.select("src_id", "dst_id").distinct().count() == m, m)
    check("flight_conservation", total(e, "num_flights") == etl.getAs[Long]("retained_records"), total(e, "num_flights"))
    check("delay_count_conservation", total(e, "delay_observations") == etl.getAs[Long]("delay_observations"), total(e, "delay_observations"))
    val directSum = Option(etl.getAs[Any]("sum_arrival_delay")).map(_.toString.toDouble).getOrElse(0.0)
    check("delay_sum_conservation", math.abs(total(e, "sum_arrival_delay") - directSum) <= 1e-8 * math.max(1.0, math.abs(directSum)), total(e, "sum_arrival_delay"))
    check("route_denominators", e.filter(col("num_flights") <= 0 || col("delay_observations") < 0 ||
      col("delay_observations") > col("num_flights") || col("delayed_flights_15min") > col("delay_observations") ||
      (col("delay_observations") === 0 && (col("avg_arrival_delay").isNotNull || col("sum_arrival_delay").isNotNull)) ||
      (col("delay_observations") > 0 && (col("avg_arrival_delay").isNull || col("sum_arrival_delay").isNull ||
        abs(col("avg_arrival_delay") * col("delay_observations") - col("sum_arrival_delay")) > 1e-6))).count() == 0, m)
    check("cleaning_funnel", total(read("audit/cleaning_funnel"), "records") == etl.getAs[Long]("raw_records"), etl.getAs[Long]("raw_records"))
    for (name <- Seq("pagerank", "weighted_pagerank", "uniform_pagerank")) {
      val scores = read(s"results/$name")
      val values = scores.select("score").as[Double].collect()
      check(s"${name}_finite_nonnegative", values.length == n && values.forall(x => x.isFinite && x >= 0), values.length)
      check(s"${name}_labels", scores.select("vertex_id", "iata").except(v.select("vertex_id", "iata")).count() == 0 && scores.select("vertex_id").distinct().count() == n, n)
      if (name != "pagerank") {
        check(s"${name}_mass", math.abs(values.sum - 1.0) < 1e-6, values.sum)
        val info = spark.read.json(Seq(Output.readJson(spark, s"${c.run}/metrics/$name.json")).toDS()).first()
        check(s"${name}_converged", info.getAs[Boolean]("converged"), info.getAs[Double]("residual"))
      }
    }
    val components = read("results/connected_components")
    check("component_membership", components.count() == n && components.select("vertex_id").distinct().count() == n &&
      total(read("results/component_sizes"), "airports") == n, components.count())
    val triangles = read("results/triangle_count")
    val triangleSum = total(triangles, "triangles").toLong
    check("triangle_divisibility", triangleSum % 3 == 0, triangleSum / 3)
    check("clustering_bounds", triangles.count() == n && triangles.filter(col("clustering").isNull || isnan(col("clustering")) ||
      col("clustering") < 0 || col("clustering") > 1.0 + 1e-10 || col("triangles") < 0).count() == 0, n)
    val paths = read("results/shortest_paths")
    check("path_grid", paths.count() == n * c.landmarks.size && paths.select("vertex_id", "landmark_id").distinct().count() == paths.count(), paths.count())
    check("path_null_semantics", paths.filter((col("reachable") && (col("hops").isNull || col("hops") < 0)) || (!col("reachable") && col("hops").isNotNull)).count() == 0, paths.count())
    check("landmark_self_distance", paths.filter(col("vertex_id") === col("landmark_id") && col("hops") === 0 && col("reachable")).count() == c.landmarks.size, c.landmarks.size)
    val airportMetrics = read("results/airport_metrics")
    check("airport_traffic_conservation", total(airportMetrics, "in_strength") == total(e, "num_flights") && total(airportMetrics, "out_strength") == total(e, "num_flights"), total(airportMetrics, "in_strength"))
    check("airport_delay_conservation", total(airportMetrics, "arrival_delay_observations") == total(e, "delay_observations") && math.abs(total(airportMetrics, "arrival_delay_sum") - directSum) <= 1e-8 * math.max(1.0, math.abs(directSum)), total(airportMetrics, "arrival_delay_observations"))
    val routeMetrics = read("results/route_metrics")
    check("route_export_matches_clean", routeMetrics.drop("delayed_fraction").except(e).count() == 0 && e.except(routeMetrics.drop("delayed_fraction")).count() == 0, routeMetrics.count())
    val monthly = read("audit/exclusions_by_month")
    check("monthly_conservation", total(monthly, "raw_records") == etl.getAs[Long]("raw_records") && total(monthly, "retained_records") == etl.getAs[Long]("retained_records"), total(monthly, "raw_records"))
    val observedSizes = components.groupBy("component_id").agg(count(lit(1)).as("airports"))
    check("component_sizes_match_membership", observedSizes.except(read("results/component_sizes")).count() == 0 && read("results/component_sizes").except(observedSizes).count() == 0, observedSizes.count())
    val passed = checks.forall(_._2)
    val rows = checks.toSeq.toDF("check", "passed", "observed")
    val json = rows.toJSON.collect().mkString(",")
    Output.json(spark, s"${c.run}/validation/validation.json", s"""{"run_id":"${c.runId}","passed":$passed,"checks":[$json]}""")
    require(passed, "Saved-output validation failed: " + checks.filterNot(_._2).map(_._1).mkString(", "))
  }
}
