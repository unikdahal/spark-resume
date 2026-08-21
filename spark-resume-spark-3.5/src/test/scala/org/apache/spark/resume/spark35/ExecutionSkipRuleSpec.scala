package org.apache.spark.resume.spark35

import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeLike}

import org.apache.spark.resume.api._
import org.apache.spark.resume.api.memory.{InMemoryAnchorStore, InMemoryExchangeStore}

/** [[ExecutionSkipRule]]'s own substitution decisions, exercised directly on the plan AQE would
  * hand it -- deliberately WITHOUT executing anything, unlike [[ExecutionSkipAcceptanceSpec]]
  * (which proves the end-to-end "same rows, fewer tasks" claim for one query and is far too coarse
  * to say WHICH exchange got substituted, or why one didn't).
  *
  * Every test here is a case where the rule's answer was previously wrong, or right only by luck:
  * the multi-exchange walk order, the partition-count precondition, the shuffle/broadcast scope,
  * and per-fragment stage numbering. None of them are reachable through the single-exchange
  * `repartition` fixture the acceptance test uses. */
class ExecutionSkipRuleSpec extends SparkTestBase {

  /** The plan AQE's `injectQueryStagePrepRule` rules are actually handed: the physical plan INSIDE
    * the `AdaptiveSparkPlanExec` wrapper, not the wrapper itself (which is a `LeafExecNode`, so a
    * rule applied to it could never reach any exchange at all -- the same `children`-is-`Nil` trap
    * `WholePlanFingerprint` documents at length). */
  private def prepPlan(df: DataFrame): SparkPlan = df.queryExecution.executedPlan match {
    case a: AdaptiveSparkPlanExec => a.initialPlan
    case p => p
  }

  /** A real, reattachable anchor for `digest`: real handle from a real `store()` call, so the
    * rule's `deserializeHandle`/`isFresh`/`checkIdentityIsolation` checks all pass on their own
    * merits. The partition BYTES are empty because no test here ever executes the substituted plan
    * -- what is being asserted is which node the rule chose, not what reading it returns. */
  private def realAnchor(
      store: InMemoryExchangeStore,
      queryId: String,
      generation: Long,
      digest: String,
      numPartitions: Int): Anchor = {
    val handle = store.store(Array.fill(numPartitions)(Array.emptyByteArray))
    Anchor(
      schemaVersion = "1",
      queryId = StageCaptureListener.stageQueryId(queryId),
      generation = generation,
      fingerprint = digest,
      handleKind = store.handleKind,
      handlePayload = store.serializeHandle(handle),
      numMappers = 1,
      numPartitions = numPartitions,
      bytesByPartition = Array.fill(numPartitions)(0L),
      rowCount = 0L,
      createdAtMs = System.currentTimeMillis())
  }

  test("an anchor matching an INNER exchange never blocks the OUTER one from being skipped") {
    withNewSession() { spark =>
      val queryId = s"nested-${UUID.randomUUID()}"
      val anchorStore = new InMemoryAnchorStore
      val exchangeStore = new InMemoryExchangeStore

      val df = spark.range(0, 100, 1, 4).repartition(3).selectExpr("id % 10 as k").groupBy("k").count()
      val plan = prepPlan(df)
      val exchanges = plan.collect { case e: ShuffleExchangeLike => e }
      exchanges should have size 2 // fixture sanity: this really is a nested two-shuffle plan

      val key = StageCaptureListener.stageQueryId(queryId)
      val generation = anchorStore.acquireGeneration(key)
      exchanges.foreach { ex =>
        val digest = WholePlanFingerprint.compute(ex, Seq.empty)
        anchorStore.putAnchor(generation,
          realAnchor(exchangeStore, queryId, generation, digest, ex.outputPartitioning.numPartitions))
      }

      val rule = new ExecutionSkipRule(queryId, anchorStore, () => exchangeStore, Seq.empty)
      val substituted = rule.apply(plan)

      // The OUTER exchange is substituted and its whole subtree -- the inner exchange included --
      // goes with it, so NO shuffle exchange remains to be executed. A bottom-up walk cannot reach
      // this state: it rewrites the inner exchange first, and the outer one's digest is then
      // computed over a subtree containing `SkippedExchangeExec` instead of the original inner
      // exchange, so it can never match the anchor captured for it. The symptom is not a crash --
      // it is one real exchange still sitting above the skipped one, quietly re-executing.
      substituted.collect { case s: SkippedExchangeExec => s } should have size 1
      substituted.collect { case e: ShuffleExchangeLike => e } shouldBe empty
    }
  }

  test("an anchor whose partition count disagrees with the plan's is refused, not substituted") {
    withNewSession() { spark =>
      val queryId = s"partition-mismatch-${UUID.randomUUID()}"
      val anchorStore = new InMemoryAnchorStore
      val exchangeStore = new InMemoryExchangeStore

      val plan = prepPlan(spark.range(0, 100, 1, 4).repartition(3).toDF())
      val ex = plan.collect { case e: ShuffleExchangeLike => e }.head
      val key = StageCaptureListener.stageQueryId(queryId)
      val generation = anchorStore.acquireGeneration(key)
      // One MORE partition than this plan will ask for. Substituting anyway is not a
      // wrong-but-harmless result: `SkippedShuffleRDD` builds its partitions from the PLAN's count,
      // so the mismatch would surface on an EXECUTOR as a read for an index the producing run never
      // wrote -- killing the user's query, past the point where falling through to normal execution
      // is still possible. This must fail closed on the driver instead.
      anchorStore.putAnchor(generation, realAnchor(exchangeStore, queryId, generation,
        WholePlanFingerprint.compute(ex, Seq.empty), ex.outputPartitioning.numPartitions + 1))

      val rule = new ExecutionSkipRule(queryId, anchorStore, () => exchangeStore, Seq.empty)
      val substituted = rule.apply(plan)

      substituted.collect { case s: SkippedExchangeExec => s } shouldBe empty
      substituted.collect { case e: ShuffleExchangeLike => e } should have size 1
    }
  }

  test("a BROADCAST exchange is never substituted, even when an anchor matches its digest") {
    withNewSession() { spark =>
      import spark.implicits._
      val queryId = s"broadcast-${UUID.randomUUID()}"
      val anchorStore = new InMemoryAnchorStore
      val exchangeStore = new InMemoryExchangeStore

      val small = Seq((1, "a"), (2, "b")).toDF("id", "v")
      val big = spark.range(0, 100).selectExpr("cast(id as int) as id")
      val plan = prepPlan(big.join(small.hint("broadcast"), "id"))
      val broadcasts = plan.collect { case b: BroadcastExchangeExec => b }
      broadcasts should have size 1 // fixture sanity: the hint really did produce a broadcast

      // An anchor the CAPTURE side would never write: `StageFingerprint.capturedStages` is scoped
      // to `ShuffleQueryStageExec` on purpose (broadcast output is an in-memory broadcast variable,
      // not shuffle-service-addressable bytes). Written here deliberately to prove the rule's own
      // scope holds by construction rather than by digests never colliding -- substituting a
      // broadcast would replace it with a node implementing only `doExecute`, and the downstream
      // `BroadcastHashJoinExec`'s `executeBroadcast()` would then throw
      // `UnsupportedOperationException` in the middle of a user query.
      val key = StageCaptureListener.stageQueryId(queryId)
      val generation = anchorStore.acquireGeneration(key)
      val broadcast = broadcasts.head
      anchorStore.putAnchor(generation, realAnchor(exchangeStore, queryId, generation,
        WholePlanFingerprint.compute(broadcast, Seq.empty),
        broadcast.outputPartitioning.numPartitions))

      val rule = new ExecutionSkipRule(queryId, anchorStore, () => exchangeStore, Seq.empty)
      val substituted = rule.apply(plan)

      substituted.collect { case s: SkippedExchangeExec => s } shouldBe empty
      substituted.collect { case b: BroadcastExchangeExec => b } should have size 1
    }
  }

  test("several exchanges in one fragment are reported with DISTINCT stage ids, never all 0") {
    withNewSession() { spark =>
      val queryId = s"stage-ids-${UUID.randomUUID()}"
      val anchorStore = new InMemoryAnchorStore
      val exchangeStore = new InMemoryExchangeStore

      val df = spark.range(0, 100, 1, 4).repartition(3).selectExpr("id % 10 as k").groupBy("k").count()
      val plan = prepPlan(df)
      val exchanges = plan.collect { case e: ShuffleExchangeLike => e }
      exchanges should have size 2

      val key = StageCaptureListener.stageQueryId(queryId)
      val generation = anchorStore.acquireGeneration(key)
      exchanges.foreach { ex =>
        anchorStore.putAnchor(generation, realAnchor(exchangeStore, queryId, generation,
          WholePlanFingerprint.compute(ex, Seq.empty), ex.outputPartitioning.numPartitions))
      }

      // Rejecting from a rule keeps every candidate visitable: nothing is substituted, so the walk
      // reaches both exchanges instead of stopping at the first one it rewrites.
      val seen = ArrayBuffer.empty[Int]
      val recorder = new AdmissionRule {
        override def evaluate(candidate: AdmissionCandidate): AdmissionVerdict = {
          seen += candidate.stageInfo.stageId
          Reject("recording only")
        }
      }
      new ExecutionSkipRule(queryId, anchorStore, () => exchangeStore, Seq.empty, Seq(recorder)).apply(plan)

      // Distinct, and specifically not "both 0" -- a rule keying on `stageId` for observability or
      // policy must be able to tell two stages of one fragment apart. `UnknownStageId` (-1) is the
      // sentinel for a node the rule could not place; a real, placeable exchange must never report
      // it, and an unplaceable one must never borrow a real stage's id.
      seen.toSeq.sorted shouldBe Seq(0, 1)
      seen should not contain ExecutionSkipRule.UnknownStageId
    }
  }
}
