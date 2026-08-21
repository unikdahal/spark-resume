package org.apache.spark.resume.integration

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

import org.apache.spark.resume.celeborn.CelebornExchangeStore
import org.apache.spark.resume.core._
import org.apache.spark.resume.fs.FsExchangeStore
import org.apache.spark.resume.redis.RedisAnchorStore
import org.apache.spark.resume.spark35.{ExecutionSkipRule, StageAdmissionCheck, StageCaptureListener}

/** The "resuming" side of this module's cross-process, cross-backend composition proof (see
  * README.md and [[ProcessA]]'s doc comment for the producing side). A separate JVM and a fresh
  * `SparkSession`.
  *
  * `INTEGRATION_SCENARIO` (default `"admitted"`) selects which real, disclosed outcome this run
  * proves composes end to end -- every scenario reaches a genuinely different terminal state via
  * REAL backend behavior, not a mocked/forced one:
  *
  *   - `"admitted"`: `resumingAppUniqueId` deliberately DIFFERENT from `ProcessA`'s
  *     `producingAppUniqueId` (exactly the split `checkIdentityIsolation` exists to guard), and
  *     the SAME query shape `ProcessA` built. Reaches `RefusedUnsupported` (`isFresh` true,
  *     `checkIdentityIsolation` OK, `reattach` throws the documented Tier 3 gap).
  *   - `"stale"`: same identity/query setup as `"admitted"`, but `ProcessA` (this scenario) wrote
  *     an anchor whose `CelebornHandle` was never actually registered. Reaches `RefusedStale` via
  *     the real backend's own `isFresh` -- `checkIdentityIsolation`/`reattach` are never reached.
  *   - `"isolation-conflict"`: `resumingAppUniqueId` set to the SAME string `ProcessA` used for
  *     `producingAppUniqueId` (deriving it identically, not reading it off the anchor -- this
  *     process has no legitimate way to know that string besides misconfiguration, which is
  *     exactly the hazard being proven). Reaches `RefusedIsolationConflict` -- `reattach` is never
  *     reached either.
  *   - `"miss"`: builds a STRUCTURALLY DIFFERENT query than `ProcessA` did (a different
  *     `repartition` count -- see `Fixture`'s doc comment for why that's a genuine, not
  *     coincidental, fingerprint miss). Every decision is `RejectedBy` (no anchor matches this
  *     fingerprint); `SafeReattach` is never even called, proving a real miss composes correctly
  *     across a real process boundary too, not just an admitted match.
  *   - `"skip"`: the one scenario that does NOT go through Celeborn, and the only one in which
  *     execution is actually skipped. `ProcessA` captures the query's REAL row bytes through
  *     `StageCaptureListener`'s `exchangeStore` path into a real, cross-process-durable
  *     `spark-resume-fs` store; this process then registers `ExecutionSkipRule` via
  *     `injectQueryStagePrepRule` and re-runs the same query shape. Asserts BOTH acceptance
  *     criteria: identical final rows to an unresumed baseline run in this same process, AND
  *     strictly fewer Spark tasks -- the cross-process counterpart of `spark-resume-spark-3.5`'s
  *     `ExecutionSkipAcceptanceSpec`, which proves the same mechanism only within one JVM.
  *
  * Every scenario asserts its OWN expected terminal state explicitly (non-zero exit / failed test
  * otherwise) -- proving everything up to the backend byte-read composes correctly across real
  * processes, and that the ONLY thing that doesn't work in the `"admitted"` case is exactly, and
  * only, the already-disclosed Celeborn Tier 3 gap. Execution is genuinely skipped in `"skip"`,
  * and deliberately not in any of the four Celeborn-backed scenarios above. */
object ProcessB {

  def main(args: Array[String]): Unit = {
    val queryId = requireEnv("INTEGRATION_QUERY_ID")
    val scenario = sys.env.getOrElse("INTEGRATION_SCENARIO", "admitted")

    if (scenario == "skip") {
      runSkipScenario(queryId)
    } else {
      runCelebornBackedScenario(queryId, scenario)
    }
  }

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

  /** `scenario=skip`: the REAL cross-process execution-skip proof this whole module exists to
    * provide -- `ProcessA` (a SEPARATE, ALREADY-EXITED process) wrote the anchor and the real row
    * bytes through `spark-resume-fs`, on disk; this process reads them back from a fresh
    * `SparkSession` with `ExecutionSkipRule` registered via `injectQueryStagePrepRule`, exactly
    * the mechanism `AqeExecutionSkipSpikeSpec`/`ExecutionSkipAcceptanceSpec`
    * (`spark-resume-spark-3.5`) proved works, now proven to survive an actual OS process boundary
    * too. Same acceptance criteria as `ExecutionSkipAcceptanceSpec`: correct final rows AND fewer
    * Spark tasks than an unresumed baseline -- both, not either. */
  private def runSkipScenario(queryId: String): Unit = {
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val fsBaseDir = requireEnv("FS_STORE_BASE_DIR")

    val redisStore = new RedisAnchorStore(redisHost, redisPort)
    // One store, named once: every anchor below is checked against the SAME backend, and the
    // resuming session's own factory builds from the same fsBaseDir.
    val exchangeStore = new FsExchangeStore(fsBaseDir)
    try {
      val anchorsBeforeResume = redisStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))
      require(anchorsBeforeResume.nonEmpty,
        "no anchors found -- ProcessA (scenario=skip) must run first and write them")
      require(anchorsBeforeResume.forall(_.handleKind == exchangeStore.handleKind),
        "ProcessA wrote at least one anchor WITHOUT a real fs handle (the NoHandleKind sentinel) " +
          s"-- byte capture degraded, so this run could not prove a skip: ${anchorsBeforeResume.map(_.handleKind)}")

      // -- Baseline: an ordinary session, no skip rule -- ground truth for correct rows AND the
      // real task count an unresumed run of the IDENTICAL query actually needs. --
      var baselineRows: Array[Long] = Array.empty
      var baselineTaskCount = 0
      val baselineSpark = SparkSession.builder().master("local[2]").appName("skip-b-baseline").getOrCreate()
      try {
        baselineTaskCount = countTasks(baselineSpark) {
          baselineRows = Fixture.query(baselineSpark).collect().map(_.getLong(0)).sorted
        }
      } finally {
        baselineSpark.stop()
      }

      // -- Resuming session: ExecutionSkipRule registered BEFORE the session is built, reading
      // real bytes from the real, cross-process-durable fs store ProcessA wrote to. --
      var resumedRows: Array[Long] = Array.empty
      var resumedTaskCount = 0
      val resumingSpark = SparkSession.builder()
        .master("local[2]")
        .appName("skip-b-resuming")
        .withExtensions((ext: SparkSessionExtensions) => ext.injectQueryStagePrepRule { _ =>
          new ExecutionSkipRule(queryId, redisStore, () => new FsExchangeStore(fsBaseDir), Seq.empty)
        })
        .getOrCreate()
      try {
        resumedTaskCount = countTasks(resumingSpark) {
          resumedRows = Fixture.query(resumingSpark).collect().map(_.getLong(0)).sorted
        }
      } finally {
        resumingSpark.stop()
      }

      println(s"[ProcessB] scenario=skip: baselineTaskCount=$baselineTaskCount resumedTaskCount=$resumedTaskCount")
      require(resumedRows.toSeq == baselineRows.toSeq,
        s"resumed rows do not match baseline -- resumed=${resumedRows.take(5).toSeq}..., baseline=${baselineRows.take(5).toSeq}...")
      require(resumedTaskCount < baselineTaskCount,
        s"expected FEWER tasks when resuming (the stage should be genuinely skipped), got resumed=$resumedTaskCount baseline=$baselineTaskCount")
      println("[ProcessB] SUCCESS (skip): real execution-skip proven across a real OS process boundary -- " +
        "correct results, fewer tasks, real bytes read from spark-resume-fs")
    } finally {
      redisStore.close()
    }
  }

  private def runCelebornBackedScenario(queryId: String, scenario: String): Unit = {
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val celebornRestBaseUrl = sys.env.getOrElse("CELEBORN_MASTER_REST", "http://localhost:9098")
    val producingAppUniqueId = s"integration-producer-$queryId" // must match ProcessA's formula
    val resumingAppUniqueId =
      if (scenario == "isolation-conflict") producingAppUniqueId else s"integration-resumer-$queryId"

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-resume-integration-process-b")
      .getOrCreate()

    try {
      // IDENTICAL source to ProcessA's in every scenario except "miss" -- NOT executed here (see
      // this object's doc comment on why that's deliberate, matching StageAdmissionCheck's own
      // "a check runs before deciding whether to execute" posture).
      val df = if (scenario == "miss") Fixture.query(spark, numPartitions = 7) else Fixture.query(spark)
      val redisStore = new RedisAnchorStore(redisHost, redisPort)
      val celebornStore = new CelebornExchangeStore(celebornRestBaseUrl, resumingAppUniqueId)
      try {
        val decisions = StageAdmissionCheck.check(df.queryExecution, queryId, redisStore, Seq.empty)
        require(decisions.nonEmpty,
          "check-side found no shuffle stage candidates -- fixture bug, not a real finding")
        println(s"[ProcessB] scenario=$scenario, ${decisions.size} stage decision(s): " +
          decisions.map(_.decision.outcome).mkString(", "))

        val admitted = decisions.filter(_.decision.outcome == Admitted)

        if (scenario == "miss") {
          require(admitted.isEmpty,
            s"expected NO Admitted decision for a structurally different query, got: ${decisions.map(_.decision.outcome).mkString(", ")}")
          val allRejectedForNoAnchor = decisions.forall {
            case StageAdmissionCheck.StageDecision(_, AdmissionDecision(_, _, _, RejectedBy(AdmissionEngine.NoAnchorRuleName, _), _)) => true
            case _ => false
          }
          require(allRejectedForNoAnchor,
            s"expected every decision rejected for lack of a matching anchor, got: ${decisions.map(_.decision.outcome).mkString(", ")}")
          println("[ProcessB] SUCCESS (miss): a structurally different query correctly found no matching " +
            "anchor across a real process boundary -- SafeReattach was never even called")
        } else {
          require(admitted.nonEmpty,
            s"expected at least one Admitted decision, got: ${decisions.map(_.decision.outcome).mkString(", ")}")

          val stageQueryId = StageCaptureListener.stageQueryId(queryId)
          val anchors = redisStore.loadAnchors(stageQueryId)

          var reachedExpectedOutcome = false
          admitted.foreach { d =>
            val anchor = anchors.find(_.fingerprint == d.decision.fingerprint).getOrElse(
              throw new IllegalStateException(
                s"Admitted decision for fingerprint ${d.decision.fingerprint} but no matching anchor on reload"))
            require(anchor.handleKind == celebornStore.handleKind,
              s"expected a real celeborn handle, got handleKind=${anchor.handleKind} -- ProcessA/ProcessB out of sync")
            val handle = celebornStore.deserializeHandle(anchor.handlePayload)
            val outcome = SafeReattach.attempt(celebornStore, handle)
            println(s"[ProcessB] SafeReattach.attempt -> $outcome")
            (scenario, outcome) match {
              case ("admitted", RefusedUnsupported(_)) => reachedExpectedOutcome = true
              case ("stale", RefusedStale) => reachedExpectedOutcome = true
              case ("isolation-conflict", RefusedIsolationConflict(_)) => reachedExpectedOutcome = true
              case (_, other) =>
                throw new IllegalStateException(s"scenario=$scenario expected a different outcome, got $other")
            }
          }
          require(reachedExpectedOutcome, s"scenario=$scenario never reached its expected outcome")
          println(s"[ProcessB] SUCCESS ($scenario): full pipeline composed end-to-end across two real " +
            "processes, a real Redis, and a real Celeborn cluster")
        }
      } finally {
        redisStore.close()
      }
    } finally {
      spark.stop()
    }
  }

  private def requireEnv(name: String): String =
    sys.env.getOrElse(name, throw new IllegalStateException(s"required env var $name not set"))
}
