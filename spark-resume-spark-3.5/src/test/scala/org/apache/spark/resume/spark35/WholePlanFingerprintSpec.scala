package org.apache.spark.resume.spark35

class WholePlanFingerprintSpec extends SparkTestBase {

  private val providers = DefaultProviders.all

  // An earlier version of this test asserted AQE-on and AQE-off fingerprint IDENTICALLY for the
  // same query/data. That assumption was wrong, found by running the test, not by inspection:
  // `WholePlanFingerprint` walks `AdaptiveSparkPlanExec.initialPlan` for an AQE-enabled query
  // (see that unwrap's own doc comment for why -- it must be a plan snapshot stable before
  // execution, for the check side to have anything to compare against at all), but
  // `initialPlan` is captured BEFORE Spark's `prepareForExecution` rules (whole-stage codegen,
  // `ColumnarToRow` insertion) run; a non-AQE query's `executedPlan` already has those applied.
  // The two plans differ in real node shape for reasons that have nothing to do with AQE
  // adaptation itself -- an artifact of WHEN each path prepares its plan, not a correctness bug.
  // Confirmed by dumping both `.toString()`s side by side, not assumed. So: this project makes NO
  // claim that an AQE-enabled and a non-AQE-enabled run of the same query fingerprint
  // identically -- an anchor captured with one AQE setting is not expected to admit a check run
  // under a different one, and that's fine, since a real resumption scenario always reruns under
  // the SAME session configuration. What actually matters, and what the tests below check
  // instead, is AQE-enabled-to-AQE-enabled stability and discrimination.
  test("AQE-enabled: the SAME query, run twice, produces the SAME whole-plan fingerprint") {
    val dir = tmpDir.resolve("aqe-stable").toString
    withNewSession(adaptiveEnabled = true) { spark =>
      import spark.implicits._
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("t")
      val fp1 = WholePlanFingerprint.compute(spark.sql("select v, count(*) from t group by v").queryExecution.executedPlan, providers)
      val fp2 = WholePlanFingerprint.compute(spark.sql("select v, count(*) from t group by v").queryExecution.executedPlan, providers)
      fp1 shouldBe fp2
    }
  }

  test("the AQE-on fingerprint genuinely walked past the wrapper: two DIFFERENT queries under AQE get DIFFERENT fingerprints") {
    val dir = tmpDir.resolve("aqe-discriminate").toString
    withNewSession(adaptiveEnabled = true) { spark =>
      import spark.implicits._
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val df1 = spark.read.parquet(dir).groupBy("v").count()
      df1.collect()
      val df2 = spark.read.parquet(dir).filter("id > 1").groupBy("v").count()
      df2.collect()
      val fp1 = WholePlanFingerprint.compute(df1.queryExecution.executedPlan, providers)
      val fp2 = WholePlanFingerprint.compute(df2.queryExecution.executedPlan, providers)
      // If the walker were silently stuck at the AdaptiveSparkPlanExec/QueryStageExec wrapper
      // (both LeafExecNode, .children == Nil), EVERY AQE query would hash to the same constant
      // and this assertion would catch it -- this is the test the AQE-on/AQE-off equality test
      // above cannot catch by itself.
      fp1 should not be fp2
    }
  }

  test("UNRECOGNIZED leaf node (no registered provider): the fallback still discriminates two different sources, not just avoid crashing") {
    withNewSession() { spark =>
      // spark.range(...) plans to RangeExec, a built-in leaf node with NO registered
      // SourceFingerprint provider in this suite -- exactly the disclosed fallback path
      // (invariant A-3: never crash; still must discriminate per A-1).
      val plan10 = spark.range(0, 10).queryExecution.executedPlan
      val plan20 = spark.range(0, 20).queryExecution.executedPlan
      noException should be thrownBy WholePlanFingerprint.compute(plan10, providers)
      val fp10 = WholePlanFingerprint.compute(plan10, providers)
      val fp20 = WholePlanFingerprint.compute(plan20, providers)
      // The real bug this test exists to catch: RangeExec's defining parameters (start/end/step)
      // are plain constructor fields, NOT Catalyst Expressions -- `.expressions` does not surface
      // them. A fallback built ONLY from `class name + exprString` (exprString empty for a node
      // with no Expression-typed fields) would silently collapse range(0,10) and range(0,20) to
      // the IDENTICAL fingerprint: the exact false-positive-resumption shape invariant A-1 exists
      // to forbid, for the class of node this project has the LEAST specific knowledge of.
      fp10 should not be fp20
    }
  }

  test("UNRECOGNIZED leaf node: the same range queried twice is stable") {
    withNewSession() { spark =>
      val p1 = spark.range(0, 15).queryExecution.executedPlan
      val p2 = spark.range(0, 15).queryExecution.executedPlan
      WholePlanFingerprint.compute(p1, providers) shouldBe WholePlanFingerprint.compute(p2, providers)
    }
  }

  test("a provider that THROWS degrades this leaf to the fallback rather than crashing the whole computation") {
    val throwingProvider = new org.apache.spark.resume.api.SourceFingerprint {
      override def supports(target: org.apache.spark.resume.api.FingerprintTarget): Boolean = true
      override def fingerprint(target: org.apache.spark.resume.api.FingerprintTarget): String =
        throw new RuntimeException("provider bug")
    }
    withNewSession() { spark =>
      val plan = spark.range(0, 5).queryExecution.executedPlan
      noException should be thrownBy WholePlanFingerprint.compute(plan, Seq(throwingProvider))
    }
  }
}
