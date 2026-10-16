import org.apache.spark.graphx._

/** Implements convergent PageRank with positive weights and dangling-mass handling. */
object WeightedPageRank {
  /** Captures the converged graph and diagnostics required by validation. */
  case class Result(graph: Graph[Double, Double], iterations: Int, residual: Double,
    converged: Boolean, mass: Double)

  /** Iterate from uniform scores until tolerance or the iteration cap is reached. */
  def run(input: Graph[String, Double], alpha: Double, tolerance: Double, maxIterations: Int): Result = {
    val n = input.vertices.count().toDouble
    require(n > 0 && input.edges.filter(e => !e.attr.isFinite || e.attr <= 0).count() == 0)
    val strengths = input.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr), _ + _)
    val weighted = input.outerJoinVertices(strengths)((_, _, s) => s.getOrElse(0.0))
    val transitions = weighted.mapTriplets(t => t.attr / t.srcAttr).cache()
    transitions.edges.count()

    // Pair each score with a flag identifying vertices that have no outgoing edge.
    var state = transitions.mapVertices((_, strength) => (1.0 / n, strength == 0.0)).cache()
    state.vertices.count()
    var iteration = 0
    var residual = Double.PositiveInfinity
    while (iteration < maxIterations && residual >= tolerance) {
      // Redistribute dangling score mass uniformly, then add weighted incoming mass.
      val dangling = state.vertices.filter(_._2._2).map(_._2._1).sum()
      val messages = state.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr._1 * ctx.attr), _ + _, TripletFields.Src)
      val next = state.outerJoinVertices(messages) { (_, old, incoming) =>
        (alpha / n + (1.0 - alpha) * (incoming.getOrElse(0.0) + dangling / n), old._2)
      }.cache()
      if ((iteration + 1) % 10 == 0) next.checkpoint()
      next.vertices.count()
      residual = state.vertices.join(next.vertices).map { case (_, (a, b)) => math.abs(a._1 - b._1) }.max()

      // Release old vertex state only after its replacement has materialized.
      state.unpersistVertices(false)
      state = next
      iteration += 1
    }
    val scores = state.mapVertices((_, value) => value._1).cache()
    val mass = scores.vertices.map(_._2).sum()
    state.unpersistVertices(false)
    transitions.unpersistVertices(false)
    Result(scores, iteration, residual, residual < tolerance, mass)
  }
}
