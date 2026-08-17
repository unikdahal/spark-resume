package org.apache.spark.resume.spark35

import org.apache.spark.sql.SparkSession

import org.apache.spark.resume.api.memory.InMemoryAnchorStore
import org.apache.spark.resume.core.Admitted

/** End-to-end across [[StageCaptureListener]] and [[StageAdmissionCheck]] -- the per-stage
  * counterpart to [[AdmissionCheckIntegrationSpec]]. Uses the same `listenerBus.waitUntilEmpty`
  * fix that spec needed (see its comment and [[SparkResumeListener]]'s doc comment for the full
  * account of why): `QueryExecutionListener` delivery is asynchronous regardless of which
  * listener is registered. */
class StageAdmissionCheckIntegrationSpec extends SparkTestBase {

  private def captureAndWait(spark: SparkSession, listener: StageCaptureListener)(query: => Unit): Unit = {
    spark.listenerManager.register(listener)
    query
    spark.sparkContext.listenerBus.waitUntilEmpty(30000)
    spark.listenerManager.unregister(listener)
  }

  test("capture then check the IDENTICAL shuffle query in a second session -> one stage, Admitted, with REAL stats") {
    val anchorStore = new InMemoryAnchorStore
    val queryId = "q-stage-admit"

    withNewSession() { spark =>
      import spark.implicits._
      val dir = tmpDir.resolve("stage-e2e-admit").toString
      Seq((1, "a"), (2, "b"), (3, "a")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val listener = new StageCaptureListener(queryId, anchorStore, DefaultProviders.all)
      captureAndWait(spark, listener) {
        spark.read.parquet(dir).groupBy("v").count().collect()
      }
    }

    // Real stats actually landed in the store, not just an admission verdict -- the whole point
    // of per-stage capture over the whole-query placeholder.
    val stored = anchorStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))
    stored should have size 1
    stored.head.numMappers should be > 0
    stored.head.bytesByPartition.sum should be > 0L

    val decisions = withNewSession() { spark =>
      val df = spark.read.parquet(tmpDir.resolve("stage-e2e-admit").toString).groupBy("v").count()
      StageAdmissionCheck.check(df.queryExecution, queryId, anchorStore, DefaultProviders.all)
    }
    decisions should have size 1
    decisions.head.decision.outcome shouldBe Admitted
  }

  test("checking a query with no matching stage anchor -> Rejected, no anchor, no exception") {
    val anchorStore = new InMemoryAnchorStore
    val decisions = withNewSession() { spark =>
      import spark.implicits._
      val dir = tmpDir.resolve("stage-e2e-miss").toString
      Seq((1, "a"), (2, "b")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val df = spark.read.parquet(dir).groupBy("v").count()
      StageAdmissionCheck.check(df.queryExecution, "never-captured", anchorStore, DefaultProviders.all)
    }
    decisions should have size 1
    decisions.head.decision.outcome match {
      case org.apache.spark.resume.core.RejectedBy(ruleName, _) =>
        ruleName shouldBe org.apache.spark.resume.core.AdmissionEngine.NoAnchorRuleName
      case other => fail(s"expected RejectedBy(NoAnchorRuleName, _), got $other")
    }
  }

  test("a query with no shuffle stages at all -> empty decision list, not an error") {
    val anchorStore = new InMemoryAnchorStore
    val decisions = withNewSession() { spark =>
      import spark.implicits._
      val dir = tmpDir.resolve("stage-e2e-noshuffle").toString
      Seq((1, "a"), (2, "b")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val df = spark.read.parquet(dir).filter("id > 1")
      StageAdmissionCheck.check(df.queryExecution, "q-noshuffle", anchorStore, DefaultProviders.all)
    }
    decisions shouldBe empty
  }
}
