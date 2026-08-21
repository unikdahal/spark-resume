package org.apache.spark.resume.spark35

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.api.memory.InMemoryAnchorStore

/** [[StageCaptureListener]] behavior that needs no `SparkSession` at all -- the capture path itself
  * is covered end to end by [[ExecutionSkipAcceptanceSpec]] and
  * [[StageAdmissionCheckIntegrationSpec]]. */
class StageCaptureListenerSpec extends AnyFunSuite with Matchers {

  test("awaitCaptures on a closed listener reports failure immediately, not as a timeout") {
    val listener = new StageCaptureListener("q-closed", new InMemoryAnchorStore, Seq.empty)
    listener.close()

    // Returning `false` is right (a closed listener cannot observe anything queued before the
    // close), but it must not claim the captures ran out of time: nothing was waited on at all.
    // Asserted by ELAPSED TIME rather than by reading the log -- the barrier submission is
    // rejected outright, so this returns at once instead of blocking for the full timeout.
    val startNs = System.nanoTime()
    listener.awaitCaptures(30000) shouldBe false
    val elapsedMs = (System.nanoTime() - startNs) / 1000000L
    elapsedMs should be < 30000L

    listener.close() // idempotent, per its own contract
  }
}
