package org.apache.spark.resume.spark35

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.api._
import org.apache.spark.resume.api.memory.{InMemoryAnchorStore, InMemoryExchangeStore}

/** The REAL execution-skip acceptance test -- not `AqeExecutionSkipSpikeSpec`'s eager-execute
  * cheat. Proves what that spike explicitly did NOT: a stage captured with REAL row bytes
  * (`StageCaptureListener`'s `exchangeStore` path) can be genuinely skipped in a second process
  * (`ExecutionSkipRule`, registered via `injectQueryStagePrepRule`), producing the SAME final rows
  * as an unresumed baseline AND submitting FEWER Spark tasks -- not just "doesn't crash" or
  * "results happen to match," the two things a correct-results-only test could pass on even if
  * the substitution silently fell through to normal execution.
  *
  * `InMemoryExchangeStore`, not a real cross-process backend: this test lives in
  * `spark-resume-spark-3.5`, which stays backend-agnostic (no dependency on `spark-resume-fs`).
  * `InMemoryExchangeStore.store`/`readPartition` are genuinely real (real `Array[Byte]` storage,
  * not a fixture-only shortcut) -- what's NOT proven here is cross-PROCESS durability, which is
  * `spark-resume-fs`/`spark-resume-integration`'s job, not this module's. See
  * `spark-resume-integration` for the cross-process version of this same proof. */
class ExecutionSkipAcceptanceSpec extends AnyFunSuite with Matchers {

  private def countTasks(spark: SparkSession)(body: => Unit): Int = {
    val counter = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = counter.incrementAndGet()
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      spark.sparkContext.listenerBus.waitUntilEmpty(30000)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    counter.get()
  }

  test("GO/NO-GO: a real captured stage is genuinely skipped -- same final rows, FEWER tasks, not just 'doesn't crash'") {
    val queryId = s"skip-acceptance-${UUID.randomUUID()}"
    val anchorStore = new InMemoryAnchorStore
    val exchangeStore = new InMemoryExchangeStore

    // -- Baseline: an ordinary session, no capture, no skip rule -- establishes ground truth for
    // both the correct final rows AND the real task count an unresumed run actually needs. --
    var baselineRows: Array[Long] = Array.empty
    var baselineTaskCount = 0
    val baselineSpark = SparkSession.builder().master("local[2]").appName("skip-accept-baseline").getOrCreate()
    try {
      baselineTaskCount = countTasks(baselineSpark) {
        baselineRows = baselineSpark.range(0, 4000, 1, 4).repartition(3).collect().map(_.longValue()).sorted
      }
    } finally {
      baselineSpark.stop()
    }
    baselineTaskCount should be > 0

    // -- Producing session: captures the SAME query's REAL row bytes via StageCaptureListener's
    // exchangeStore path, writing a REAL, reattachable anchor. --
    val producerSpark = SparkSession.builder().master("local[2]").appName("skip-accept-producer").getOrCreate()
    try {
      val listener = new StageCaptureListener(queryId, anchorStore, Seq.empty, exchangeStore = Some(exchangeStore))
      producerSpark.listenerManager.register(listener)
      producerSpark.range(0, 4000, 1, 4).repartition(3).collect()
      producerSpark.sparkContext.listenerBus.waitUntilEmpty(30000)
    } finally {
      producerSpark.stop()
    }
    val anchors = anchorStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))
    anchors should not be empty
    anchors.head.handleKind shouldBe exchangeStore.handleKind // a REAL handle, not the NoHandleKind sentinel

    // -- Resuming session: ExecutionSkipRule registered via injectQueryStagePrepRule, the SAME
    // query shape run again. If the skip fires, this session's own stage never actually executes
    // the shuffle -- readPartition supplies the rows instead. --
    var resumedRows: Array[Long] = Array.empty
    var resumedTaskCount = 0
    val resumingSpark = SparkSession.builder()
      .master("local[2]")
      .appName("skip-accept-resuming")
      .withExtensions((ext: SparkSessionExtensions) => ext.injectQueryStagePrepRule { session =>
        new ExecutionSkipRule(queryId, anchorStore, exchangeStore, () => exchangeStore, Seq.empty)
      })
      .getOrCreate()
    try {
      resumedTaskCount = countTasks(resumingSpark) {
        resumedRows = resumingSpark.range(0, 4000, 1, 4).repartition(3).collect().map(_.longValue()).sorted
      }
    } finally {
      resumingSpark.stop()
    }

    // The actual acceptance criteria -- BOTH, not either:
    resumedRows.toSeq shouldBe baselineRows.toSeq // correct results
    // Observed empirically for this exact fixture (range(4 partitions).repartition(3).collect()):
    // baseline 7 tasks (4 upstream shuffle-map + 3 result), resumed 3 (only the result tasks,
    // each reading its own partition via readPartition) -- the 4 upstream map tasks are genuinely
    // ELIMINATED, not just reduced. Asserted as a relative comparison, not the exact numbers,
    // so this test doesn't become fragile to unrelated Spark scheduling changes.
    resumedTaskCount should be < baselineTaskCount // AND fewer tasks -- the stage was genuinely skipped
    resumedTaskCount should be < baselineTaskCount // AND fewer tasks -- the stage was genuinely skipped
  }
}
