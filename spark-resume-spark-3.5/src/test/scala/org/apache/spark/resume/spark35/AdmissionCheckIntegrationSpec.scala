package org.apache.spark.resume.spark35

import org.apache.spark.sql.SparkSession

import org.apache.spark.resume.api.memory.{InMemoryAnchorStore, InMemoryExchangeStore}
import org.apache.spark.resume.core.{Admitted, RejectedBy}

/** End-to-end across the capture half ([[SparkResumeListener]]) and the check half
  * ([[AdmissionCheck]]) -- the actual decision-layer proof Phase 1 exists to deliver (see this
  * module's doc comments and README for what it deliberately does NOT prove: real byte-level
  * reattachment, which needs a Tier 2/3 backend this phase has none of). */
class AdmissionCheckIntegrationSpec extends SparkTestBase {

  private def newAnchorStore = new InMemoryAnchorStore
  private def newExchangeStore = new InMemoryExchangeStore

  /** Registers `listener`, runs `query`, and -- the fix for a real, intermittently-reproducing
    * bug found by running this suite in a loop, not by a single run -- waits for Spark's
    * asynchronous listener bus to fully drain before unregistering. `QueryExecutionListener`
    * events are posted through `SparkContext`'s own async `LiveListenerBus`, not delivered
    * synchronously with the call that triggers them (see [[SparkResumeListener]]'s doc comment
    * for the full account); skipping this wait races the dispatch and can silently lose the
    * capture with no exception anywhere. */
  private def captureAndWait(spark: SparkSession, listener: SparkResumeListener)(query: => Unit): Unit = {
    spark.listenerManager.register(listener)
    query
    spark.sparkContext.listenerBus.waitUntilEmpty(30000)
    spark.listenerManager.unregister(listener)
  }

  test("capture then check the IDENTICAL query in a second session -> Admitted") {
    val dir = tmpDir.resolve("e2e-admit").toString
    val anchorStore = newAnchorStore
    val exchangeStore = newExchangeStore
    val queryId = "q-admit"

    withNewSession() { spark =>
      import spark.implicits._
      Seq((1, "a"), (2, "b")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val listener = new SparkResumeListener(queryId, anchorStore, exchangeStore, DefaultProviders.all)
      captureAndWait(spark, listener) {
        spark.read.parquet(dir).groupBy("v").count().collect()
      }
    }

    val decision = withNewSession() { spark =>
      val df = spark.read.parquet(dir).groupBy("v").count()
      AdmissionCheck.check(df.queryExecution, queryId, anchorStore, DefaultProviders.all)
    }
    decision.outcome shouldBe Admitted
  }

  test("capture, then check a DIFFERENT query (never captured) against the SAME queryId -> Rejected, no anchor") {
    val dir = tmpDir.resolve("e2e-reject-different-query").toString
    val anchorStore = newAnchorStore
    val exchangeStore = newExchangeStore
    val queryId = "q-reject"

    withNewSession() { spark =>
      import spark.implicits._
      Seq((1, "a"), (2, "b")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val listener = new SparkResumeListener(queryId, anchorStore, exchangeStore, DefaultProviders.all)
      captureAndWait(spark, listener) {
        spark.read.parquet(dir).groupBy("v").count().collect() // captures THIS query's fingerprint
      }
    }

    val decision = withNewSession() { spark =>
      // A structurally different query against the same data -- filter changes the plan shape,
      // so this must NOT match the anchor captured above.
      val df = spark.read.parquet(dir).filter("id > 1").groupBy("v").count()
      AdmissionCheck.check(df.queryExecution, queryId, anchorStore, DefaultProviders.all)
    }
    decision.outcome match {
      case RejectedBy(_, _) => // expected
      case other => fail(s"expected RejectedBy, got $other")
    }
  }

  test("GO/NO-GO: capture, mutate the underlying file, then check the IDENTICAL query text -> Rejected, not silently Admitted") {
    val dir = tmpDir.resolve("e2e-mutate").toString
    val anchorStore = newAnchorStore
    val exchangeStore = newExchangeStore
    val queryId = "q-mutate"

    withNewSession() { spark =>
      import spark.implicits._
      Seq((1, "a"), (2, "b")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val listener = new SparkResumeListener(queryId, anchorStore, exchangeStore, DefaultProviders.all)
      captureAndWait(spark, listener) {
        spark.read.parquet(dir).groupBy("v").count().collect()
      }
    }

    withNewSession() { spark =>
      import spark.implicits._
      // Same path, materially different content -- the exact scenario this whole project exists
      // to refuse: identical query TEXT, table mutated underneath the anchor.
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")).toDF("id", "v")
        .write.mode("overwrite").parquet(dir)
    }

    val decision = withNewSession() { spark =>
      val df = spark.read.parquet(dir).groupBy("v").count()
      AdmissionCheck.check(df.queryExecution, queryId, anchorStore, DefaultProviders.all)
    }
    decision.outcome match {
      case RejectedBy(_, _) => // expected: this is the correctness property, not an edge case
      case other => fail(s"expected RejectedBy (stale digest must not be silently admitted), got $other")
    }
  }

  test("checking a queryId nothing was ever captured for -> Rejected, no anchor, no exception") {
    val anchorStore = newAnchorStore
    val decision = withNewSession() { spark =>
      val df = spark.range(0, 5)
      AdmissionCheck.check(df.queryExecution, "never-captured", anchorStore, DefaultProviders.all)
    }
    decision.outcome match {
      case RejectedBy(ruleName, _) =>
        ruleName shouldBe org.apache.spark.resume.core.AdmissionEngine.NoAnchorRuleName
      case other => fail(s"expected RejectedBy(NoAnchorRuleName, _), got $other")
    }
  }

  test("a driver-supplied AdmissionRule participates in the decision -- e.g. rejecting on a business-level condition even with a matching anchor") {
    val dir = tmpDir.resolve("e2e-custom-rule").toString
    val anchorStore = newAnchorStore
    val exchangeStore = newExchangeStore
    val queryId = "q-custom-rule"

    withNewSession() { spark =>
      import spark.implicits._
      Seq((1, "a"), (2, "b")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val listener = new SparkResumeListener(queryId, anchorStore, exchangeStore, DefaultProviders.all)
      captureAndWait(spark, listener) {
        spark.read.parquet(dir).groupBy("v").count().collect()
      }
    }

    val alwaysReject = new org.apache.spark.resume.api.AdmissionRule {
      override def evaluate(candidate: org.apache.spark.resume.api.AdmissionCandidate) =
        org.apache.spark.resume.api.Reject("operator policy: resumption disabled for this queryId")
    }

    val decision = withNewSession() { spark =>
      val df = spark.read.parquet(dir).groupBy("v").count()
      AdmissionCheck.check(df.queryExecution, queryId, anchorStore, DefaultProviders.all, Seq(alwaysReject))
    }
    decision.outcome match {
      case RejectedBy(ruleName, reason) =>
        ruleName should include("$anon") // an anonymous rule class -- still a real rule name, not the built-in one
        reason should include("operator policy")
      case other => fail(s"expected RejectedBy from the custom rule, got $other")
    }
  }
}
