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
  * @param exchangeStore Phase 4 addition, `None` by default -- when `None` (Phase 2's original,
  *   unchanged behavior), `handleKind`/`handlePayload` are the disclosed sentinel
  *   [[StageCaptureListener.NoHandleKind]] / empty bytes: proves per-stage IDENTITY and
  *   STATISTICS only, no live reattach path, exactly as Phase 2 shipped. When `Some(store)`, this
  *   listener ALSO reads each captured stage's REAL row bytes (via `CapturedStage.plan.execute()`
  *   -- cheap, not a recomputation, see that field's own doc comment) and calls `store.store(...)`
  *   to write a REAL, reattachable handle -- the producer-side half of real execution-skipping
  *   (see `ExecutionSkipRule`). `rowCount` is real in this path too (counted from the actual rows
  *   encoded), unlike the `None` path's `0` placeholder. */
final class StageCaptureListener(
    queryId: String,
    anchorStore: AnchorStore,
    providers: Seq[SourceFingerprint],
    exchangeStore: Option[ExchangeStore] = None)
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
          val (handleKind, handlePayload, rowCount) = exchangeStore match {
            case Some(store) =>
              try {
                val partitionBytes = captureRealPartitionBytes(s)
                val handle = store.store(partitionBytes)
                val realRowCount = countRows(partitionBytes)
                (store.handleKind, store.serializeHandle(handle), realRowCount)
              } catch {
                // A-3 at THIS stage's granularity: a byte-capture failure for one stage must not
                // block writing the other stages' anchors, and must not silently claim a real
                // handle it doesn't have -- degrade to the same disclosed sentinel the `None` path
                // uses, not a broken/partial real handle.
                case NonFatal(_) => (StageCaptureListener.NoHandleKind, Array.emptyByteArray, 0L)
              }
            case None => (StageCaptureListener.NoHandleKind, Array.emptyByteArray, 0L)
          }
          val anchor = Anchor(
            schemaVersion = "1",
            queryId = key,
            generation = generation,
            fingerprint = s.digest,
            handleKind = handleKind,
            handlePayload = handlePayload,
            numMappers = s.numMappers,
            numPartitions = s.numPartitions,
            bytesByPartition = s.bytesByPartition,
            rowCount = rowCount,
            createdAtMs = System.currentTimeMillis())
          anchorStore.putAnchor(generation, anchor)
        }
      }
    } catch {
      case NonFatal(_) => // A-3 applied to the integration layer: degrade silently, never crash.
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()

  /** Reads `stage.plan`'s ACTUAL materialized output, one real `Array[Byte]` per partition, via
    * `RowBytesCodec.encode`. Runs a small job (`.mapPartitionsWithIndex(...).collect()`) against
    * an ALREADY-materialized exchange -- cheap, re-reading already-computed shuffle output, not
    * recomputing anything upstream (see `CapturedStage.plan`'s own doc comment). */
  private def captureRealPartitionBytes(s: CapturedStage): Array[Array[Byte]] = {
    val indexed = s.plan.execute().mapPartitionsWithIndex { (i, rows) =>
      Iterator.single((i, RowBytesCodec.encode(rows)))
    }.collect()
    val out = new Array[Array[Byte]](s.numPartitions)
    indexed.foreach { case (i, bytes) => out(i) = bytes }
    // A partition Spark's own RDD produced zero tasks/output for (a genuinely empty partition,
    // not a bug) still needs a real, present (if empty) entry -- readPartition's own contract
    // forbids treating a missing entry as "no data" ambiguously.
    for (i <- out.indices if out(i) == null) out(i) = Array.emptyByteArray
    out
  }

  private def countRows(partitionBytes: Array[Array[Byte]]): Long =
    partitionBytes.map(RowBytesCodec.countRows).sum
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
