package org.apache.spark.resume.integration

import org.apache.spark.sql.SparkSession

import org.apache.spark.resume.celeborn.CelebornExchangeStore
import org.apache.spark.resume.core._
import org.apache.spark.resume.redis.RedisAnchorStore
import org.apache.spark.resume.spark35.{StageAdmissionCheck, StageCaptureListener}

/** The "resuming" side of this module's cross-process, cross-backend composition proof (see
  * README.md and [[ProcessA]]'s doc comment for the producing side). A separate JVM, a fresh
  * `SparkSession`, and a `resumingAppUniqueId` deliberately DIFFERENT from `ProcessA`'s
  * `producingAppUniqueId` -- exactly the identity split `CelebornExchangeStore.checkIdentityIsolation`
  * exists to guard.
  *
  * Builds the SAME query [[ProcessA]] built (see [[Fixture]]) but does NOT execute it: `df
  * .queryExecution` is read before any action runs, matching `StageAdmissionCheck`'s own
  * documented "a check runs before deciding whether to execute at all" posture -- there is no
  * "how AQE would adapt it" to read yet, by construction, on this side.
  *
  * Expects the terminal outcome to be `RefusedUnsupported`, and asserts exactly that (non-zero
  * exit otherwise) -- that is the honest end state this whole pipeline reaches today: real
  * cross-process fingerprint match, real cross-process anchor load, a real `AdmissionEngine`
  * `Admitted` decision, and `SafeReattach.attempt` reaching all the way to the real Celeborn
  * cluster before being refused by the one documented, checked Tier 3 gap
  * (`CelebornExchangeStore.reattach`). A test asserting `RefusedUnsupported` at the end of a full
  * real pipeline is stronger evidence than three modules each passing alone: it proves everything
  * up to the backend byte-read composes correctly across processes, and that the one thing that
  * doesn't work is exactly, and only, the already-disclosed gap -- not a silent success, and not a
  * raw crash either. */
object ProcessB {

  def main(args: Array[String]): Unit = {
    val queryId = requireEnv("INTEGRATION_QUERY_ID")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val celebornRestBaseUrl = sys.env.getOrElse("CELEBORN_MASTER_REST", "http://localhost:9098")
    val resumingAppUniqueId = s"integration-resumer-$queryId"

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-resume-integration-process-b")
      .getOrCreate()

    try {
      val df = Fixture.query(spark) // IDENTICAL source to ProcessA's -- NOT executed here.
      val redisStore = new RedisAnchorStore(redisHost, redisPort)
      val celebornStore = new CelebornExchangeStore(celebornRestBaseUrl, resumingAppUniqueId)
      try {
        val decisions = StageAdmissionCheck.check(df.queryExecution, queryId, redisStore, Seq.empty)
        require(decisions.nonEmpty,
          "check-side found no shuffle stage candidates -- fixture bug, not a real finding")
        println(s"[ProcessB] ${decisions.size} stage decision(s): " +
          decisions.map(_.decision.outcome).mkString(", "))

        val admitted = decisions.filter(_.decision.outcome == Admitted)
        require(admitted.nonEmpty,
          s"expected at least one Admitted decision, got: ${decisions.map(_.decision.outcome).mkString(", ")}")

        val stageQueryId = StageCaptureListener.stageQueryId(queryId)
        val anchors = redisStore.loadAnchors(stageQueryId)

        var sawRefusedUnsupported = false
        admitted.foreach { d =>
          val anchor = anchors.find(_.fingerprint == d.decision.fingerprint).getOrElse(
            throw new IllegalStateException(
              s"Admitted decision for fingerprint ${d.decision.fingerprint} but no matching anchor on reload"))
          require(anchor.handleKind == celebornStore.handleKind,
            s"expected a real celeborn handle, got handleKind=${anchor.handleKind} -- ProcessA/ProcessB out of sync")
          val handle = celebornStore.deserializeHandle(anchor.handlePayload)
          SafeReattach.attempt(celebornStore, handle) match {
            case RefusedUnsupported(reason) =>
              println(s"[ProcessB] SafeReattach.attempt -> RefusedUnsupported: $reason")
              sawRefusedUnsupported = true
            case other =>
              throw new IllegalStateException(
                s"expected RefusedUnsupported (the real, checked Tier 3 gap), got $other")
          }
        }
        require(sawRefusedUnsupported,
          "no admitted, reattachable stage was found to exercise SafeReattach against")
        println("[ProcessB] SUCCESS: full pipeline composed end-to-end across two real processes, " +
          "a real Redis, and a real Celeborn cluster")
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
