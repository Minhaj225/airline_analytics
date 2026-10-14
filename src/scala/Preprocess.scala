import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Preprocess {
  def csv(spark: SparkSession, path: String, required: Seq[String]): DataFrame = {
    val header = spark.read.option("header", "true").csv(path).columns.toSeq
    require(required.forall(header.contains) && header.distinct.size == header.size,
      s"Missing or duplicate required CSV columns in $path")
    val schema = StructType(header.map(StructField(_, StringType, true)) :+
      StructField("_corrupt_record", StringType, true))
    spark.read.schema(schema).option("header", "true").option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record").csv(path)
  }
  def run(spark: SparkSession, c: ProjectConfig): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
    val airports = csv(spark, s"${c.raw}/airports.csv",
      Seq("IATA_CODE", "AIRPORT", "CITY", "STATE", "COUNTRY", "LATITUDE", "LONGITUDE"))
      .withColumn("IATA_CODE", upper(trim(col("IATA_CODE")))).cache()
    airports.count()
    require(airports.filter(col("_corrupt_record").isNotNull || !coalesce(col("IATA_CODE").rlike("^[A-Z]{3}$"), lit(false))).count() == 0,
      "Corrupt airport lookup or invalid key")
    require(airports.groupBy("IATA_CODE").count().filter(col("count") > 1).count() == 0,
      "Duplicate airport lookup keys (including conflicting metadata) are not allowed")
    val airlines = csv(spark, s"${c.raw}/airlines.csv", Seq("IATA_CODE", "AIRLINE")).cache()
    airlines.count()
    require(airlines.filter(col("IATA_CODE").isNull || col("_corrupt_record").isNotNull).count() == 0 &&
      airlines.groupBy("IATA_CODE").count().filter(col("count") > 1).count() == 0, "Invalid airline lookup")
    val raw = csv(spark, s"${c.raw}/flights.csv", Seq("YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER",
      "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "ARRIVAL_DELAY", "CANCELLED", "DIVERTED")).cache()
    val rawCount = raw.count()
    val validDate = udf((y: String, m: String, d: String) => {
      try { y != null && y.matches("2015") && m.matches("[0-9]{1,2}") && d.matches("[0-9]{1,2}") &&
        java.time.LocalDate.of(y.toInt, m.toInt, d.toInt).getYear == 2015 }
      catch { case _: Exception => false }
    })
    val numericDelay = col("ARRIVAL_DELAY").cast(DoubleType)
    val prepared = raw.withColumn("origin_iata", upper(trim(col("ORIGIN_AIRPORT"))))
      .withColumn("destination_iata", upper(trim(col("DESTINATION_AIRPORT"))))
      .withColumn("delay", when(!isnan(numericDelay) && abs(numericDelay) =!= lit(Double.PositiveInfinity), numericDelay))
      .join(airports.select(col("IATA_CODE").as("origin_lookup")), col("origin_iata") === col("origin_lookup"), "left")
      .join(airports.select(col("IATA_CODE").as("destination_lookup")), col("destination_iata") === col("destination_lookup"), "left")
    val dateFlags = validDate(col("YEAR"), col("MONTH"), col("DAY")) &&
      coalesce(col("CANCELLED").isin("0", "1") && col("DIVERTED").isin("0", "1"), lit(false))
    val iata = coalesce(col("origin_iata").rlike("^[A-Z]{3}$") && col("destination_iata").rlike("^[A-Z]{3}$"), lit(false))
    val numeric = coalesce(col("origin_iata").rlike("^[0-9]+$") || col("destination_iata").rlike("^[0-9]+$"), lit(false))
    val audited = prepared.withColumn("reason",
      when(col("_corrupt_record").isNotNull, "corrupt")
        .when(!dateFlags, "invalid_date_or_flags")
        .when(col("CANCELLED") =!= "0" || col("DIVERTED") =!= "0", "cancelled_or_diverted")
        .when(!iata && numeric, "numeric_airport")
        .when(!iata, "malformed_airport")
        .when(col("origin_iata") === col("destination_iata"), "self_loop")
        .when(col("origin_lookup").isNull || col("destination_lookup").isNull, "unmatched_airport")
        .otherwise("retained")).cache()
    audited.count()
    val reasons = Seq("corrupt", "invalid_date_or_flags", "cancelled_or_diverted", "numeric_airport",
      "malformed_airport", "self_loop", "unmatched_airport", "retained")
    val reasonCounts = audited.groupBy("reason").count().as[(String, Long)].collect().toMap
    var remaining = rawCount
    val funnel = reasons.zipWithIndex.map { case (reason, index) =>
      val n = reasonCounts.getOrElse(reason, 0L)
      if (reason != "retained") remaining -= n
      (index + 1, reason, n, remaining)
    }.toDF("step", "reason", "records", "remaining_records")
    Output.table(funnel, c, "audit/cleaning_funnel")
    Output.table(audited.groupBy("YEAR", "MONTH").agg(count(lit(1)).as("raw_records"),
      sum(when(col("reason") === "retained", 1L).otherwise(0L)).as("retained_records"),
      sum(when(col("reason") === "numeric_airport", 1L).otherwise(0L)).as("numeric_excluded_records")), c, "audit/exclusions_by_month")
    Output.table(audited.filter(col("reason") === "unmatched_airport")
      .groupBy("origin_iata", "destination_iata").count(), c, "audit/unmatched_airports")
    val retained = audited.filter(col("reason") === "retained").cache()
    val retainedCount = retained.count()
    require(retainedCount > 0, "No retained flights; inspect cleaning audit")
    val active = retained.select(col("origin_iata").as("iata"))
      .union(retained.select(col("destination_iata").as("iata"))).distinct()
    require(active.count() <= 10000, "Airport mapping exceeds bounded mapping limit")
    val vertices = active.withColumn("vertex_id", row_number().over(Window.orderBy("iata")).cast(LongType))
      .join(airports, col("iata") === col("IATA_CODE"))
      .select(col("vertex_id"), col("iata"), col("AIRPORT").as("airport_name"), col("CITY").as("city"),
        col("STATE").as("state"), col("COUNTRY").as("country"),
        col("LATITUDE").cast(DoubleType).as("latitude"), col("LONGITUDE").cast(DoubleType).as("longitude")).cache()
    val routes = retained.groupBy("origin_iata", "destination_iata").agg(
      count(lit(1)).as("num_flights"), count(col("delay")).as("delay_observations"),
      sum("delay").as("sum_arrival_delay"), avg("delay").as("avg_arrival_delay"),
      sum(when(col("delay") >= 15.0, 1L).otherwise(0L)).as("delayed_flights_15min"))
    val edges = routes.join(vertices.select(col("iata").as("origin_iata"), col("vertex_id").as("src_id")), Seq("origin_iata"))
      .join(vertices.select(col("iata").as("destination_iata"), col("vertex_id").as("dst_id")), Seq("destination_iata")).cache()
    Output.table(vertices, c, "clean/vertices")
    Output.table(edges, c, "clean/edges")
    val sums = retained.agg(count(col("delay")), sum("delay")).first()
    val delaySum = if (sums.isNullAt(1)) "null" else sums.getDouble(1).toString
    val invalidDelays = retained.filter(col("ARRIVAL_DELAY").isNotNull && length(trim(col("ARRIVAL_DELAY"))) > 0 && col("delay").isNull).count()
    val duplicates = rawCount - raw.drop("_corrupt_record").distinct().count()
    val unknownAirlines = retained.join(airlines, retained("AIRLINE") === airlines("IATA_CODE"), "left_anti").count()
    val missingCoordinates = vertices.filter(col("latitude").isNull || col("longitude").isNull || isnan(col("latitude")) || isnan(col("longitude"))).count()
    val missingNames = vertices.filter(col("airport_name").isNull || length(trim(col("airport_name"))) === 0).count()
    val undirectedEdges = edges.select(least(col("src_id"), col("dst_id")).as("a"), greatest(col("src_id"), col("dst_id")).as("b")).distinct().count()
    Output.json(spark, s"${c.run}/metrics/coverage.json",
      s"""{"run_id":"${c.runId}","active_airports_missing_coordinates":$missingCoordinates,"active_airports_missing_names":$missingNames,"undirected_edges":$undirectedEdges}""")
    Output.json(spark, s"${c.run}/metrics/etl.json",
      s"""{"run_id":"${c.runId}","raw_records":$rawCount,"retained_records":$retainedCount,"vertices":${vertices.count()},"directed_edges":${edges.count()},"delay_observations":${sums.getLong(0)},"sum_arrival_delay":$delaySum,"invalid_retained_delays":$invalidDelays,"exact_duplicate_records":$duplicates,"unknown_airline_records":$unknownAirlines,"inactive_lookup_airports":${airports.count() - vertices.count()}}""")
    Seq(raw, audited, retained, vertices, edges, airports, airlines).foreach(_.unpersist())
  }
}
