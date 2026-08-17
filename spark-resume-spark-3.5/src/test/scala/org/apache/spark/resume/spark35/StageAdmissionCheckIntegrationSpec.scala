package org.apache.spark.resume.spark35

import org.apache.spark.sql.SparkSession

import org.apache.spark.resume.api.memory.{InMemoryAnchorStore, InMemoryExchangeStore}
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

    // `Admitted` here means "identity + stats matched," never "safe to reattach" -- see
    // StageAdmissionCheck's doc comment. Prove it two ways, not just assert it: the convenience
    // check says no, AND a real ExchangeStore independently refuses the payload if a caller
    // ignored that and tried anyway (A-2's fail-closed posture enforced at the store layer).
    StageAdmissionCheck.isReattachable(stored.head) shouldBe false
    an[IllegalArgumentException] should be thrownBy
      new InMemoryExchangeStore().deserializeHandle(stored.head.handlePayload)
  }

  test("two structurally IDENTICAL exchanges -> both decisions Admitted, no crash on the digest collision") {
    val anchorStore = new InMemoryAnchorStore
    val queryId = "q-stage-collision"

    withNewSession() { spark =>
      spark.conf.set("spark.sql.exchange.reuse", "false") // see StageFingerprintSpec for why
      import spark.implicits._
      val dir = tmpDir.resolve("stage-e2e-collision").toString
      Seq((1, "a"), (2, "b"), (3, "a")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val listener = new StageCaptureListener(queryId, anchorStore, DefaultProviders.all)
      captureAndWait(spark, listener) {
        val q = spark.read.parquet(dir).groupBy("v").count()
        q.union(q).collect()
      }
    }
    // Two stages with the SAME digest were captured (confirmed directly in StageFingerprintSpec)
    // -- but only ONE anchor survives here, not two. That's `InMemoryAnchorStore`'s own documented
    // design (`putAnchor`: "keyed by fingerprint: a second write for the same fingerprint in the
    // same generation overwrites"), not a `StageCaptureListener` bug -- found by this exact
    // assertion failing at `size 2` before being corrected to match reality. `AnchorStore`'s
    // contract does not actually specify which behavior (dedupe vs. keep-both) an implementation
    // must choose for same-fingerprint writes within one generation; a store that RPUSHes instead
    // (RedisAnchorStore) would keep both. Both behaviors are safe for this project's purposes --
    // a digest collision means the two anchors are semantically interchangeable (see
    // StageFingerprintSpec's reasoning), so admission only ever needs ONE matching anchor to
    // proceed, whichever store-specific behavior produced.
    val stored = anchorStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))
    stored should have size 1

    val decisions = withNewSession() { spark =>
      spark.conf.set("spark.sql.exchange.reuse", "false")
      val q = spark.read.parquet(tmpDir.resolve("stage-e2e-collision").toString).groupBy("v").count()
      StageAdmissionCheck.check(q.union(q).queryExecution, queryId, anchorStore, DefaultProviders.all)
    }
    // Both candidates resolve, both find a matching anchor (StageAdmissionCheck.check's `.find`
    // picking either of the two same-digest anchors), neither throws or silently drops -- the
    // benign-collision claim, verified end to end rather than only at the digest layer.
    decisions should have size 2
    decisions.foreach(_.decision.outcome shouldBe Admitted)
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
