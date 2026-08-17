package org.apache.spark.resume.spark35

/** The correlation claim [[StageFingerprint]]'s doc comment makes -- that a check-side digest
  * (computed from `initialPlan`, before any query runs) matches the capture-side digest for the
  * SAME materialized stage (computed from the final, post-execution plan) -- is the one thing
  * this whole mechanism is worthless without. This suite proves it by ACTUALLY RUNNING a query
  * and comparing the two digests, not by inspecting the code that computes them. */
class StageFingerprintSpec extends SparkTestBase {

  test("check-side candidate digest for a real shuffle query equals the capture-side materialized stage digest") {
    withNewSession() { spark =>
      import spark.implicits._
      val dir = tmpDir.resolve("stagefp-match").toString
      Seq((1, "a"), (2, "b"), (3, "a")).toDF("id", "v").write.mode("overwrite").parquet(dir)

      // Check side: build the query, read the raw plan, WITHOUT executing anything yet --
      // exactly how AdmissionCheck is used in practice (see AdmissionCheckIntegrationSpec).
      val checkDf = spark.read.parquet(dir).groupBy("v").count()
      val candidates = StageFingerprint.checkSideCandidates(checkDf.queryExecution.executedPlan, DefaultProviders.all)
      candidates should have size 1

      // Capture side: run a SEPARATE instance of the identical query to completion, read the
      // final plan.
      val captureDf = spark.read.parquet(dir).groupBy("v").count()
      captureDf.collect()
      val stages = StageFingerprint.capturedStages(captureDf.queryExecution.executedPlan, DefaultProviders.all)
      stages should have size 1

      // The actual claim: content-addressed match, not a coincidence of running the same code
      // twice -- these came from two independently-planned DataFrames.
      candidates.head.digest shouldBe stages.head.digest
    }
  }

  test("check-side candidate digest for a STRUCTURALLY DIFFERENT query does not match") {
    withNewSession() { spark =>
      import spark.implicits._
      val dir = tmpDir.resolve("stagefp-mismatch").toString
      Seq((1, "a"), (2, "b"), (3, "a")).toDF("id", "v").write.mode("overwrite").parquet(dir)

      val captureDf = spark.read.parquet(dir).groupBy("v").count()
      captureDf.collect()
      val captured = StageFingerprint.capturedStages(captureDf.queryExecution.executedPlan, DefaultProviders.all)
      captured should have size 1

      // A different grouping key -> a genuinely different exchange subtree.
      val checkDf = spark.read.parquet(dir).filter("id > 1").groupBy("v").count()
      val candidates = StageFingerprint.checkSideCandidates(checkDf.queryExecution.executedPlan, DefaultProviders.all)

      candidates.map(_.digest) should not contain captured.head.digest
    }
  }

  test("captured stage carries REAL runtime statistics, not placeholders") {
    withNewSession() { spark =>
      import spark.implicits._
      val dir = tmpDir.resolve("stagefp-stats").toString
      Seq((1, "a"), (2, "b"), (3, "a"), (4, "c")).toDF("id", "v")
        .write.mode("overwrite").parquet(dir)

      val df = spark.read.parquet(dir).groupBy("v").count()
      df.collect()
      val stages = StageFingerprint.capturedStages(df.queryExecution.executedPlan, DefaultProviders.all)

      stages should have size 1
      val s = stages.head
      s.numMappers should be > 0
      s.numPartitions should be > 0
      // mapStats is populated for a stage that actually ran to completion in this suite's
      // single-JVM local[2] harness -- see StageFingerprint's doc comment for why this is read
      // defensively (Option) rather than assumed in general.
      s.bytesByPartition should have length s.numPartitions
      s.bytesByPartition.sum should be > 0L
    }
  }

  test("a query with NO shuffle (single-file scan, no aggregation) yields zero stage candidates and zero captured stages") {
    withNewSession() { spark =>
      import spark.implicits._
      val dir = tmpDir.resolve("stagefp-noshuffle").toString
      Seq((1, "a"), (2, "b")).toDF("id", "v").write.mode("overwrite").parquet(dir)

      val df = spark.read.parquet(dir).filter("id > 1")
      df.collect()
      // Whether or not AQE even wraps this plan in an AdaptiveSparkPlanExec at all (it may
      // decline to, for a plan with no exchange or subquery to adapt -- a real, disclosed
      // difference from the shuffle-query case, found by this exact test failing on an assumed
      // cast before this was written defensively) is exactly the case both entry points below
      // are required to handle without the caller knowing which one it got.
      StageFingerprint.checkSideCandidates(df.queryExecution.executedPlan, DefaultProviders.all) shouldBe empty
      StageFingerprint.capturedStages(df.queryExecution.executedPlan, DefaultProviders.all) shouldBe empty
    }
  }

  test("a join+aggregation query: 2 check-side candidates (shuffle + broadcast exchange), but only 1 captured shuffle stage -- the broadcast one has no match, by design") {
    withNewSession() { spark =>
      import spark.implicits._
      val dir1 = tmpDir.resolve("join-t1").toString
      val dir2 = tmpDir.resolve("join-t2").toString
      Seq((1, "a"), (2, "b"), (3, "a")).toDF("id", "v").write.mode("overwrite").parquet(dir1)
      Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "w").write.mode("overwrite").parquet(dir2)

      val checkDf = spark.read.parquet(dir1).join(spark.read.parquet(dir2), "id").groupBy("v").count()
      val candidates = StageFingerprint.checkSideCandidates(checkDf.queryExecution.executedPlan, DefaultProviders.all)
      // One from the final shuffle (the groupBy), one from the broadcast join build side.
      candidates should have size 2

      val captureDf = spark.read.parquet(dir1).join(spark.read.parquet(dir2), "id").groupBy("v").count()
      captureDf.collect()
      val captured = StageFingerprint.capturedStages(captureDf.queryExecution.executedPlan, DefaultProviders.all)
      // StageFingerprint is deliberately scoped to ShuffleQueryStageExec only (see its doc
      // comment) -- the broadcast exchange never becomes a captured stage at all.
      captured should have size 1

      // The shuffle candidate's digest matches the one real captured stage; the broadcast
      // candidate's digest legitimately has no match anywhere in captured -- StageAdmissionCheck
      // must handle that as an ordinary miss (Rejected, no anchor), not a crash or a wrong match.
      candidates.map(_.digest) should contain(captured.head.digest)
    }
  }

  test("two structurally IDENTICAL, independently-materialized exchanges over the same source produce the SAME digest -- a real, reasoned-safe collision, not a bug") {
    withNewSession() { spark =>
      // Exchange reuse (Spark's own ReuseExchange rule) is disabled specifically so Spark
      // actually materializes BOTH branches independently instead of deduplicating them itself
      // -- with reuse left on (the default), Spark's final plan wraps the second occurrence in a
      // ReusedExchange node, which is structurally different from a plain Exchange and would
      // never collide with the first one's digest, masking the scenario this test exists to
      // exercise. Found by running it both ways, not assumed.
      spark.conf.set("spark.sql.exchange.reuse", "false")
      import spark.implicits._
      val dir = tmpDir.resolve("union-t1").toString
      Seq((1, "a"), (2, "b"), (3, "a")).toDF("id", "v").write.mode("overwrite").parquet(dir)
      val q = spark.read.parquet(dir).groupBy("v").count()
      val df = q.union(q) // the identical subquery, appearing twice
      df.collect()

      val captured = StageFingerprint.capturedStages(df.queryExecution.executedPlan, DefaultProviders.all)
      captured should have size 2
      captured.map(_.digest).distinct should have size 1 // the actual collision, confirmed real

      // Why this is safe rather than a hidden hazard: a digest collision under THIS project's
      // content-addressed scheme (leaf fingerprints hash actual file identity, not just shape --
      // see FileSourceFingerprint) can only happen when the two subtrees really did read the same
      // files through the same operators -- precisely the condition Spark's OWN ReuseExchange
      // rule exists to detect and physically merge. Two anchors landing under the same digest are
      // interchangeable records of the same underlying computation, not two different facts
      // competing for one slot -- StageAdmissionCheck.check picking either one via `.find` is a
      // reasoned, not accidental, safe choice for this reason.
    }
  }
}
