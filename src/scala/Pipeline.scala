object Pipeline {
  def run(spark: SparkSession, stage: String): Unit = {
    val c = ProjectConfig.fromEnvironment()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", sys.env("SHUFFLE_PARTITIONS"))
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.sparkContext.setCheckpointDir(s"${c.run}/checkpoint")
    if (sys.env("STORAGE") == "hdfs") {
      val scheme = new Path(c.run).getFileSystem(spark.sparkContext.hadoopConfiguration).getUri.getScheme
      require(scheme == "hdfs", s"Expected HDFS storage, found $scheme")
    }
    val started = System.nanoTime()
    stage match {
      case "preprocess" => Preprocess.run(spark, c)
      case "analytics" => GraphAnalytics.run(spark, c)
      case "validate" => Validation.run(spark, c)
      case "test" => GraphCorrectness.run(spark, c)
      case "smoke" =>
        import spark.implicits._
        Seq((1L, "GraphX available")).toDF("id", "label").write.parquet(s"${c.run}/smoke")
        require(spark.read.parquet(s"${c.run}/smoke").count() == 1)
        require(org.apache.spark.graphx.Graph != null)
      case _ => throw new IllegalArgumentException(s"Unknown stage: $stage")
    }
    val seconds = (System.nanoTime() - started) / 1e9
    Output.json(spark, s"${c.run}/metrics/$stage-stage.json",
      s"""{"run_id":"${c.runId}","stage":"$stage","seconds":$seconds,"application_id":"${spark.sparkContext.applicationId}","spark_version":"${spark.version}","scala_version":"${scala.util.Properties.versionNumberString}","java_version":"${System.getProperty("java.version")}","hadoop_client_version":"${org.apache.hadoop.util.VersionInfo.getVersion}"}""")
    Output.json(spark, s"${c.run}/markers/$stage.json", s"""{"run_id":"${c.runId}","stage":"$stage","status":"complete"}""")
  }
}
