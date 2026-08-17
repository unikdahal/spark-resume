package org.apache.spark.resume.spark35

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.exchange.Exchange
import org.scalatest.funsuite.AnyFunSuite

/** NOT production code, not wired into any public API here -- a disclosed SPIKE proving a
  * specific, load-bearing claim WRONG: docs/DESIGN.md §8 (as of the commit before this file was
  * added) stated Spark 3.5 exposes no public AQE extension point early enough to actually skip a
  * stage's execution, and that the per-stage granularity this project would need for real
  * execution-skipping ("Tier 2/3") was blocked on that absence. This spike proves the opposite,
  * against real Spark 3.5.9, not documentation: `SparkSessionExtensions.injectQueryStagePrepRule`
  * (present since Spark 3.0, confirmed against actual 3.5.1 source, not assumed from newer
  * version docs) runs on the whole plan ONCE, at `AdaptiveSparkPlanExec` construction, BEFORE any
  * `Exchange` is turned into a `QueryStageExec` and materialized -- exactly the moment a stage
  * could still be intercepted and skipped instead of executed.
  *
  * Two separate, real gates checked here, not assumed:
  *
  *   1. Does the injected rule see the SAME plan shape `StageAdmissionCheck` already computes
  *      digests from? Confirmed: the injected rule's own `WholePlanFingerprint.compute` on the
  *      `Exchange` it observes produces the IDENTICAL digest `StageFingerprint.checkSideCandidates`
  *      computes independently on `queryExecution.executedPlan` for the same query, before this
  *      probe ever calls `.collect()`. If these had NOT matched, the whole mechanism would be
  *      moot -- there would be no way to know WHICH stage the hook is looking at.
  *   2. Does a LEAF replacement for an `Exchange` (children = Nil, so
  *      `AdaptiveSparkPlanExec.createQueryStages`'s generic `case _ => if (plan.children.isEmpty)`
  *      branch treats it as ALREADY materialized, with ZERO new query stages created -- no task
  *      submission for that subtree at all) survive Spark's own validation and produce CORRECT
  *      final results downstream? Confirmed: `EnsureRequirements`/`ValidateSparkPlan` (both fixed,
  *      built-in `queryStagePreparationRules` that run BEFORE any injected rule in that list) have
  *      already validated the ORIGINAL plan by the time this spike's rule runs and swaps the
  *      Exchange out -- so a leaf declaring the SAME `output`/`outputPartitioning`/`outputOrdering`
  *      as what it replaces is never re-validated against a distribution requirement it might
  *      violate, and a downstream aggregation over the substituted subtree still sums to the
  *      exact correct row count.
  *
  * What this spike deliberately does NOT prove, and does not claim to: [[SkipExchangeExec]] here
  * cheats by eagerly calling `ex.execute()` INSIDE the rule and wrapping the result -- it still
  * executes the real shuffle once, synchronously, during plan preparation. That isolates "does the
  * structural substitution work at all" from the separate, much larger, not-yet-built question of
  * actually reading real partition bytes back from a real `ExchangeStore.reattach` result inside
  * `doExecute()` instead. That is real, substantial follow-on engineering (a backend-aware RDD,
  * careful interaction with AQE's re-optimization-on-new-stats loop, columnar/broadcast variants)
  * -- not started here, and not something this spike should be read as having already solved. */
private[spark35] case class SkipExchangeExec(
    capturedRdd: RDD[InternalRow],
    override val output: Seq[Attribute],
    override val outputPartitioning: Partitioning,
    override val outputOrdering: Seq[SortOrder]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = capturedRdd
}

class AqeExecutionSkipSpikeSpec extends AnyFunSuite {

  test("gate 1: injectQueryStagePrepRule sees the plan BEFORE materialization, and its digest " +
    "matches StageAdmissionCheck's -- otherwise the hook is useless, no matter how early it runs") {
    val seenDigests = scala.collection.mutable.ArrayBuffer.empty[String]

    class ProbeRule(spark: SparkSession) extends Rule[SparkPlan] {
      override def apply(plan: SparkPlan): SparkPlan = {
        plan.foreach {
          case ex: Exchange => seenDigests += WholePlanFingerprint.compute(ex, Seq.empty)
          case _ =>
        }
        plan // pass through unchanged -- this gate only observes, doesn't substitute
      }
    }

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("aqe-skip-spike-gate-1")
      .withExtensions((ext: SparkSessionExtensions) => ext.injectQueryStagePrepRule(s => new ProbeRule(s)))
      .getOrCreate()

    try {
      val df = spark.range(0, 4000, 1, 4).repartition(3)
      val checkDigests = StageFingerprint.checkSideCandidates(df.queryExecution.executedPlan, Seq.empty).map(_.digest)
      df.collect() // triggers AdaptiveSparkPlanExec construction -> queryStagePrepRules -> ProbeRule
      assert(seenDigests.toSet == checkDigests.toSet,
        s"gate 1 FAILED: rule saw ${seenDigests.toSet}, StageAdmissionCheck computed ${checkDigests.toSet}")
    } finally {
      spark.stop()
    }
  }

  test("gate 2: a leaf substitute for Exchange survives Spark's own validation and produces " +
    "CORRECT final results downstream, not just 'doesn't crash'") {
    var replacedCount = 0

    class SkipRule(spark: SparkSession) extends Rule[SparkPlan] {
      override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
        case ex: Exchange =>
          val rdd = ex.execute() // the deliberate cheat -- see this file's doc comment
          replacedCount += 1
          SkipExchangeExec(rdd, ex.output, ex.outputPartitioning, ex.outputOrdering)
      }
    }

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("aqe-skip-spike-gate-2")
      .withExtensions((ext: SparkSessionExtensions) => ext.injectQueryStagePrepRule(s => new SkipRule(s)))
      .getOrCreate()

    try {
      import spark.implicits._
      val df = spark.range(0, 4000, 1, 4).repartition(3, $"id").groupBy(($"id" % 5).as("bucket")).count()
      val rows = df.collect()
      val total = rows.map(_.getLong(1)).sum
      assert(replacedCount >= 1, "the rule never fired -- this gate is uninformative, not passing for the right reason")
      assert(total == 4000L, s"expected total row count 4000, got $total -- WRONG RESULTS after substitution")
    } finally {
      spark.stop()
    }
  }
}
