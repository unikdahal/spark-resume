package org.apache.spark.resume.spark35

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.Exchange

import org.apache.spark.resume.api.SourceFingerprint

/** One check-side candidate: an `Exchange` node found in `initialPlan` before any stage exists,
  * with the fingerprint of the subtree rooted at that exchange (see [[StageFingerprint]]'s doc
  * comment for why the digest, not the tree position, is the correlation key). */
final case class StageCandidate(digest: String, path: String)

/** One capture-side materialized stage, with the REAL runtime statistics Spark itself recorded
  * for it -- not the whole-query placeholder [[SparkResumeListener]] is forced to write.
  * `bytesByPartition` is empty when `mapStats` was never populated (see the doc comment on why
  * that happens and is not a bug). */
final case class CapturedStage(digest: String, numMappers: Int, numPartitions: Int,
                                bytesByPartition: Array[Long])

/** Per-stage fingerprinting: the Tier-1-only degradation of the Tier 2 extension point
  * docs/DESIGN.md §8 proposes and Spark does not yet expose. There is no public hook that fires
  * at a stage's own materialization moment, so this project cannot be TOLD when a stage
  * materializes -- instead it fingerprints the SAME exchange subtree from both sides of the
  * capture/check split and matches them by content, exactly as [[WholePlanFingerprint]] already
  * does for the whole query, just re-rooted at every `Exchange` node instead of only the plan
  * root.
  *
  * The correlation key is deliberately the subtree DIGEST, not the tree PATH (i.e. not "the third
  * child of the second exchange"). This was verified empirically, not assumed (see this module's
  * test suite): the check side reads `AdaptiveSparkPlanExec.initialPlan` (pre-execution, no
  * `ColumnarToRowExec`/`AQEShuffleReadExec` wrapper nodes exist yet), while the capture side reads
  * the FINAL, post-execution plan, where every materialized `ShuffleQueryStageExec` is reached
  * through an `AQEShuffleReadExec` wrapper that has no counterpart in `initialPlan` at all --
  * position-based matching breaks on that extra tree level alone, before even accounting for
  * `ColumnarToRowExec` insertion under the exchange itself. [[WholePlanFingerprint]]'s walker
  * treats both of those, plus `InputAdapter`/`WholeStageCodegenExec` (whole-stage-codegen's own
  * execution-detail wrappers), as transparent: they contribute nothing to the digest and are
  * unwrapped before recursing. That is what makes the same logical exchange subtree hash
  * IDENTICALLY whether read pre-execution (no such wrappers exist) or post-execution (wrapped in
  * all of them) -- content-addressing sidesteps the tree-shape mismatch entirely instead of
  * trying to keep two different walk orders in lockstep.
  *
  * Both entry points below take the RAW top-level plan exactly as `QueryExecution.executedPlan`
  * returns it -- the same convention `WholePlanFingerprint.compute` uses -- and unwrap
  * `AdaptiveSparkPlanExec` internally, deliberately to DIFFERENT inner plans on each side
  * ([[checkSideCandidates]] to `initialPlan`, [[capturedStages]] to `executedPlan`): a first-draft
  * version of `capturedStages` called `.collect` directly on the raw top-level plan and silently
  * found nothing, because `AdaptiveSparkPlanExec` is itself a `LeafExecNode` (`.children` is
  * always `Nil` on it, same trap `WholePlanFingerprint`'s own doc comment already warns about)
  * -- found by this module's test suite asserting a non-empty result, not by inspection. A query
  * AQE declines to wrap at all (no exchange or subquery in it) passes the raw plan straight
  * through unchanged on both sides, which is correct: [[checkSideCandidates]] then finds no
  * `Exchange` nodes and [[capturedStages]] finds no stages, in agreement.
  *
  * Deliberately scoped to `ShuffleQueryStageExec` only, not `BroadcastQueryStageExec`: a
  * broadcast stage's output is an in-memory broadcast variable, not shuffle-service-addressable
  * bytes -- reusing it is a different mechanism from this project's `ExchangeStore`/shuffle-bytes
  * reuse entirely, and out of scope here (see docs/DESIGN.md §3). */
object StageFingerprint {

  private def unwrapToInitial(topPlan: SparkPlan): SparkPlan = topPlan match {
    case a: AdaptiveSparkPlanExec => a.initialPlan
    case p => p
  }

  private def unwrapToExecuted(topPlan: SparkPlan): SparkPlan = topPlan match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case p => p
  }

  /** Every `Exchange` node reachable in `topPlan`'s pre-execution shape, in pre-order, with the
    * fingerprint of the subtree rooted at (and including) that exchange node itself. Call this on
    * the CHECK side, before any query has run, with `queryExecution.executedPlan` directly. */
  def checkSideCandidates(topPlan: SparkPlan, providers: Seq[SourceFingerprint]): Seq[StageCandidate] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[StageCandidate]
    def go(plan: SparkPlan, path: String): Unit = {
      plan match {
        case ex: Exchange => buf += StageCandidate(WholePlanFingerprint.compute(ex, providers), path)
        case _ =>
      }
      plan.children.zipWithIndex.foreach { case (c, i) => go(c, s"$path.$i") }
    }
    go(unwrapToInitial(topPlan), "0")
    buf.toSeq
  }

  /** Every `ShuffleQueryStageExec` materialized in `topPlan`'s FINAL, post-execution shape, with
    * the same digest [[checkSideCandidates]] would compute for the exchange subtree it grew from,
    * plus the real runtime statistics Spark recorded while running it. Call this from a
    * `QueryExecutionListener.onSuccess`, with `qe.executedPlan` directly, AFTER the query has run
    * (unlike `WholePlanFingerprint.compute`'s whole-query identity, real per-stage stats only
    * exist on the executed side). */
  def capturedStages(topPlan: SparkPlan, providers: Seq[SourceFingerprint]): Seq[CapturedStage] =
    unwrapToExecuted(topPlan).collect {
      case s: ShuffleQueryStageExec =>
        val digest = WholePlanFingerprint.compute(s.plan, providers)
        // `numMappers`/`numPartitions` are real, public accessors on `ShuffleExchangeLike` --
        // never placeholders. `mapStats` is `None` until the shuffle has actually run to
        // completion on this stage; a `ShuffleQueryStageExec` only ever reaches this collector
        // post-`onSuccess`, so in practice it is populated, but is read defensively rather than
        // assumed (a skipped/reused stage in a more complex plan is a real, if rare, case where
        // it would not be).
        val bytesByPartition = s.mapStats.map(_.bytesByPartitionId).getOrElse(Array.empty[Long])
        CapturedStage(digest, s.shuffle.numMappers, s.shuffle.numPartitions, bytesByPartition)
    }
}
