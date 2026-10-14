import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

case class ProjectConfig(raw: String, run: String, mode: String, runId: String,
  graphPartitions: Int, alpha: Double, baselineIterations: Int, tolerance: Double,
  maxIterations: Int, landmarks: Seq[String]) extends Serializable

object ProjectConfig {
  def fromEnvironment(): ProjectConfig = {
    def e(k: String): String = sys.env.getOrElse(k, throw new IllegalArgumentException(s"Missing $k"))
    val c = ProjectConfig(e("RAW_URI"), e("RUN_URI"), e("MODE"), e("RUN_ID"),
      e("GRAPH_PARTITIONS").toInt, e("PR_RESET_PROB").toDouble,
      e("BASELINE_PR_ITERATIONS").toInt, e("WEIGHTED_PR_TOL").toDouble,
      e("WEIGHTED_PR_MAX_ITERATIONS").toInt, e("LANDMARK_IATA").split(",").toSeq)
    require(c.alpha > 0 && c.alpha < 1 && c.tolerance > 0 && c.tolerance < 1)
    require(c.maxIterations > 0 && c.graphPartitions > 0 && c.baselineIterations > 0)
    require(c.landmarks.nonEmpty && c.landmarks.distinct == c.landmarks && c.landmarks.forall(_.matches("[A-Z]{3}")))
    c
  }
}

object Output {
  def table(df: DataFrame, c: ProjectConfig, path: String): Unit = {
    df.write.mode("errorifexists").parquet(s"${c.run}/$path")
    // Multiple part files are retained; the exporter merges headers correctly.
    df.write.mode("errorifexists").option("header", "true").csv(s"${c.run}/csv/$path")
  }
  def json(spark: SparkSession, path: String, value: String): Unit = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out = fs.create(p, false)
    try out.write((value + "\n").getBytes("UTF-8")) finally out.close()
  }
  def readJson(spark: SparkSession, path: String): String = {
    val p = new Path(path)
    val in = p.getFileSystem(spark.sparkContext.hadoopConfiguration).open(p)
    try scala.io.Source.fromInputStream(in, "UTF-8").mkString finally in.close()
  }
}
