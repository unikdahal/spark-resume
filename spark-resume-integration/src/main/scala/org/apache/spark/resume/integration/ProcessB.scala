package org.apache.spark.resume.integration

import org.apache.spark.sql.SparkSession

import org.apache.spark.resume.celeborn.CelebornExchangeStore
import org.apache.spark.resume.core._
import org.apache.spark.resume.redis.RedisAnchorStore
import org.apache.spark.resume.spark35.{StageAdmissionCheck, StageCaptureListener}

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
  *
  * Every scenario asserts its OWN expected terminal state explicitly (non-zero exit / failed test
  * otherwise) -- proving everything up to the backend byte-read composes correctly across real
  * processes, and that the ONLY thing that doesn't work in the `"admitted"` case is exactly, and
  * only, the already-disclosed Tier 3 gap. No execution is skipped in any scenario. */
object ProcessB {

  def main(args: Array[String]): Unit = {
    val queryId = requireEnv("INTEGRATION_QUERY_ID")
    val scenario = sys.env.getOrElse("INTEGRATION_SCENARIO", "admitted")
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
