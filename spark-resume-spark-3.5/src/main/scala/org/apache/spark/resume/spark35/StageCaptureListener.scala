package org.apache.spark.resume.spark35

import scala.util.control.NonFatal

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.spark.resume.api._

/** The capture half of per-stage fingerprinting -- see [[StageFingerprint]] for the mechanism and
  * [[StageAdmissionCheck]] for the other half. Writes one [[Anchor]] per materialized shuffle
  * stage found by [[StageFingerprint.capturedStages]], carrying REAL runtime statistics
  * (`numMappers`, `numPartitions`, `bytesByPartition` straight from Spark's own
  * `MapOutputStatistics`) -- unlike [[SparkResumeListener]]'s whole-query anchor, which is forced
  * to write honest placeholders because it has no per-stage hook to read real numbers from.
  *
  * Deliberately stored under a DIFFERENT [[AnchorStore]] key than the whole-query capture
  * ([[StageCaptureListener.stageQueryId]], not `queryId` itself) rather than mixed into the same
  * bucket: `AnchorStore.loadAnchors` returns every anchor ever written for a key with no way to
  * tell a whole-query anchor apart from a per-stage one except by re-deriving which fingerprint
  * space it came from, and giving each its own namespace makes that free instead of implicit.
  *
  * NOT reattachable yet, and says so plainly rather than pretending otherwise: this phase has no
  * `ExchangeStore`/backend wired to per-stage anchors, so `handleKind`/`handlePayload` are the
  * disclosed sentinel [[StageCaptureListener.NoHandleKind]] / empty bytes -- this listener proves
  * per-stage IDENTITY and STATISTICS only, not a live reattach path (see docs/DESIGN.md sec 14's
  * Phase 2/3 split). `rowCount` is similarly not meaningful at stage granularity (a stage's own
  * output row count is not among the runtime statistics this phase reads) and recorded as `0`. */
final class StageCaptureListener(
    queryId: String,
    anchorStore: AnchorStore,
    providers: Seq[SourceFingerprint])
    extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    // Same A-3 posture as SparkResumeListener: never let a capture-path bug fail or slow down
    // the query it's observing.
    try {
      val stages = StageFingerprint.capturedStages(qe.executedPlan, providers)
      if (stages.nonEmpty) {
        val key = StageCaptureListener.stageQueryId(queryId)
        // One generation per query completion, shared by every stage anchor from THIS run --
        // they are all facts about the same attempt, not independent writers that need their own
        // fences against each other.
        val generation = anchorStore.acquireGeneration(key)
        stages.foreach { s =>
          val anchor = Anchor(
            schemaVersion = "1",
            queryId = key,
            generation = generation,
            fingerprint = s.digest,
            handleKind = StageCaptureListener.NoHandleKind,
            handlePayload = Array.emptyByteArray,
            numMappers = s.numMappers,
            numPartitions = s.numPartitions,
            bytesByPartition = s.bytesByPartition,
            rowCount = 0L,
            createdAtMs = System.currentTimeMillis())
          anchorStore.putAnchor(generation, anchor)
        }
      }
    } catch {
      case NonFatal(_) => // A-3 applied to the integration layer: degrade silently, never crash.
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}

object StageCaptureListener {

  /** Disclosed sentinel, not a real backend tag: no [[ExchangeStore]] is wired to per-stage
    * anchors in this phase, so there is no live handle to hold a real `handleKind` for. A caller
    * inspecting an `Anchor` with this `handleKind` must not attempt to deserialize its
    * `handlePayload` through any `ExchangeStore` -- there is nothing to reattach. */
  val NoHandleKind: String = "none/identity-and-stats-only"

  /** The [[AnchorStore]] key per-stage anchors for a given whole-query `queryId` are stored
    * under -- see this class's doc comment for why it is deliberately not `queryId` itself. */
  def stageQueryId(queryId: String): String = s"$queryId::stage"
}
