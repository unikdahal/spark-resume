package org.apache.spark.resume.spark35

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
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
  * ==Capture runs OFF the listener bus thread==
  * `onSuccess` does no work itself beyond handing the executed plan to this listener's own
  * single-threaded executor. That is not an optimization, it is a correctness requirement:
  * `ExecutionListenerBus` dispatches `onSuccess` on the shared `LiveListenerBus` queue thread, so
  * doing the capture inline would block that thread for the entire duration of a Spark job --
  * every `SparkListenerTaskEnd`/`SparkListenerJobEnd` for that job and for every other concurrent
  * query in the session queues up behind it, and once the queue passes
  * `spark.scheduler.listenerbus.eventqueue.capacity` (default 10000) Spark DROPS events silently.
  * Anything counting tasks off the bus (this project's own acceptance tests, the Spark UI, metrics)
  * would then be reading numbers Spark quietly discarded. `maxCaptureBytes` bounds bytes; nothing
  * bounds how long a job takes, so the only safe posture is not to hold that thread at all.
  *
  * The cost of that is that a capture is no longer complete when the listener bus is drained.
  * A caller that must observe the anchors it just captured (a test, or a producer process about to
  * exit) waits with [[awaitCaptures]] AFTER `listenerBus.waitUntilEmpty` -- the bus wait guarantees
  * the event reached this listener, `awaitCaptures` guarantees the work it queued finished. Both,
  * in that order; neither alone is sufficient.
  *
  * @param exchangeStore Phase 4 addition, `None` by default -- when `None` (Phase 2's original,
  *   unchanged behavior), `handleKind`/`handlePayload` are the disclosed sentinel
  *   [[StageCaptureListener.NoHandleKind]] / empty bytes: proves per-stage IDENTITY and
  *   STATISTICS only, no live reattach path, exactly as Phase 2 shipped. When `Some(store)`, this
  *   listener ALSO reads each captured stage's REAL row bytes (via `CapturedStage.plan.execute()`
  *   -- cheap, not a recomputation, see that field's own doc comment) and calls `store.store(...)`
  *   to write a REAL, reattachable handle -- the producer-side half of real execution-skipping
  *   (see `ExecutionSkipRule`). `rowCount` is real in this path too (counted from the actual rows
  *   encoded), unlike the `None` path's `0` placeholder.
  * @param maxCaptureBytes ceiling on the encoded bytes ONE stage may pull through the driver --
  *   see `captureRealPartitionBytes`.
  * @param maxSessionCaptureBytes ceiling on the encoded bytes this listener will pull through the
  *   driver in TOTAL, across every query it ever observes -- see
  *   [[StageCaptureListener.DefaultMaxSessionCaptureBytes]]. */
final class StageCaptureListener(
    queryId: String,
    anchorStore: AnchorStore,
    providers: Seq[SourceFingerprint],
    exchangeStore: Option[ExchangeStore] = None,
    maxCaptureBytes: Long = StageCaptureListener.DefaultMaxCaptureBytes,
    maxSessionCaptureBytes: Long = StageCaptureListener.DefaultMaxSessionCaptureBytes)
    extends QueryExecutionListener with Logging with AutoCloseable {

  /** Running total of encoded bytes this listener has pulled through the driver, for the whole
    * lifetime of the listener -- what `maxSessionCaptureBytes` is enforced against. */
  private val sessionCapturedBytes = new AtomicLong(0L)

  /** Single-threaded and daemon: single so captures never overlap each other (two concurrent
    * collects would each be individually under the ceiling and jointly over it), daemon so a
    * caller who forgets [[close]] cannot keep the JVM alive. */
  private val captureExecutor: ExecutorService =
    Executors.newSingleThreadExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"spark-resume-stage-capture-$queryId")
        t.setDaemon(true)
        t
      }
    })

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    // Read on THIS thread (a cheap already-forced lazy val), captured by the task -- so nothing
    // downstream has to reason about whether `qe` is still safe to touch later.
    val executedPlan = qe.executedPlan
    // Same A-3 posture as SparkResumeListener: never let a capture-path bug fail or slow down
    // the query it's observing. Submission itself can only fail if this listener was closed.
    try {
      captureExecutor.execute(new Runnable {
        override def run(): Unit = capture(executedPlan)
      })
    } catch {
      case NonFatal(e) =>
        logWarning(s"spark-resume: dropped a stage capture for queryId=$queryId -- the capture " +
          "executor rejected it (listener already closed?)", e)
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()

  /** Blocks until every capture queued before this call has finished, or `timeoutMs` elapses;
    * `true` if they finished. Implemented as a FIFO barrier on the single capture thread (a no-op
    * task submitted behind the real ones), so it needs no bookkeeping that could itself drift out
    * of sync with what was actually queued.
    *
    * MUST be preceded by `sparkContext.listenerBus.waitUntilEmpty(...)` to be meaningful: this
    * waits for work already handed to the executor, and only the bus wait guarantees the
    * `onSuccess` that queues that work has been delivered at all (`QueryExecutionListener`
    * delivery is asynchronous -- see [[SparkResumeListener]]'s doc comment for the full account of
    * that race, which cost this project a real, intermittently-failing test). */
  def awaitCaptures(timeoutMs: Long): Boolean =
    try {
      captureExecutor.submit(new Runnable { override def run(): Unit = () })
        .get(timeoutMs, TimeUnit.MILLISECONDS)
      true
    } catch {
      case NonFatal(e) =>
        logWarning(s"spark-resume: captures for queryId=$queryId did not finish within ${timeoutMs}ms", e)
        false
    }

  /** Releases the capture thread. Already-queued captures still run; new ones are rejected (and
    * logged). Safe to call more than once. */
  override def close(): Unit = captureExecutor.shutdown()

  private def capture(executedPlan: SparkPlan): Unit = {
    try {
      val stages = StageFingerprint.capturedStages(executedPlan, providers)
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
                // uses, not a broken/partial real handle. Logged, not swallowed outright: every
                // refusal on this path (an oversized stage, an exhausted session budget, unknown
                // statistics) carries an operator-actionable message that is worthless if nothing
                // ever prints it, and the resulting anchor looks exactly like Phase 2's
                // identity-and-stats-only one.
                case NonFatal(e) =>
                  logWarning(s"spark-resume: byte capture degraded to ${StageCaptureListener.NoHandleKind} " +
                    s"for one stage of queryId=$queryId (digest=${s.digest}) -- this stage cannot be " +
                    "skipped on resume", e)
                  (StageCaptureListener.NoHandleKind, Array.emptyByteArray, 0L)
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
      case NonFatal(e) =>
        // A-3 applied to the integration layer: degrade, never crash -- but say so, since a
        // permanently unreachable AnchorStore is otherwise indistinguishable from a query that
        // simply had no shuffle stages in it.
        logWarning(s"spark-resume: stage capture failed for queryId=$queryId -- no anchors written " +
          "for this query", e)
    }
  }

  /** Reads `stage.plan`'s ACTUAL materialized output, one real `Array[Byte]` per partition, via
    * `RowBytesCodec.encode`. Runs a job against an ALREADY-materialized exchange -- it re-reads
    * already-computed shuffle output rather than recomputing anything upstream (see
    * `CapturedStage.plan`'s own doc comment).
    *
    * ==This pulls the stage's ENTIRE output through the driver==
    * Every partition's encoded bytes land in driver heap, so the cost is proportional to the whole
    * stage's output -- and because this runs from a `QueryExecutionListener`, it happens for every
    * successful query in the session, not only ones that will ever be resumed. Two ceilings bound
    * that, both enforced against the bytes ACTUALLY ALLOCATED ON THE DRIVER, accumulated in the
    * job's own result handler as partitions arrive: `maxCaptureBytes` for this one stage, and
    * `maxSessionCaptureBytes` for everything this listener has captured so far. Exceeding either
    * throws from the result handler, which fails (and so cancels) the job in flight -- the caller
    * turns that into the same disclosed `NoHandleKind` sentinel any other capture failure produces,
    * so an oversized stage degrades to identity-and-stats-only instead of taking the driver down
    * with an `OutOfMemoryError` (an `Error`, not `NonFatal`, so no catch in this class would
    * contain it).
    *
    * ==Why the ceiling is not enforced against `bytesByPartition` alone==
    * It used to be, and that was a unit error: `MapOutputStatistics.bytesByPartitionId` counts
    * SERIALIZED and (with `spark.shuffle.compress` on, the default) COMPRESSED shuffle-write bytes,
    * while what this method allocates is `RowBytesCodec.encode` output -- raw `UnsafeRow` bytes
    * plus a 4-byte length prefix per row, uncompressed, commonly several times larger. A stage
    * comfortably under a 256 MiB check on the compressed number could still allocate more than a
    * default 1g driver heap. Spark's statistics are still consulted first, because they are free
    * and already recorded, but only as a LOWER bound: a stage they already prove is too big is
    * refused before a job is submitted at all, and everything else is bounded by real measurement.
    *
    * A stage whose `bytesByPartition` is empty is refused ONLY when it also has mappers. Spark
    * leaves `mapStats` unpopulated when the shuffle's input RDD has zero partitions -- "nothing was
    * written," a size that is genuinely KNOWN to be zero, not unknown -- and refusing that case
    * turned a legitimately empty stage into a `NoHandleKind` anchor, which this project's own
    * skip-scenario guards treat as a hard failure. With `numMappers > 0` and no statistics the size
    * really is unknown, and unknown still fails closed. */
  private def captureRealPartitionBytes(s: CapturedStage): Array[Array[Byte]] = {
    val budget = captureBudget(s)
    val rdd = s.plan.execute()
    if (rdd.getNumPartitions != s.numPartitions) {
      // Fails closed rather than writing a handle whose partition count disagrees with the anchor's
      // -- ExecutionSkipRule refuses such a pairing on the resuming side anyway, and an anchor that
      // can never be admitted is worse than an honest NoHandleKind one.
      throw new IllegalStateException(
        s"StageCaptureListener: refusing to capture stage bytes -- the materialized RDD has " +
          s"${rdd.getNumPartitions} partitions but the stage reports ${s.numPartitions}")
    }
    val out = new Array[Array[Byte]](s.numPartitions)
    var captured = 0L
    rdd.sparkContext.runJob(
      rdd,
      (_: TaskContext, rows: Iterator[InternalRow]) => RowBytesCodec.encode(rows),
      rdd.partitions.indices,
      (partitionId: Int, bytes: Array[Byte]) => {
        captured += bytes.length
        if (captured > budget) {
          throw new IllegalStateException(
            s"StageCaptureListener: refusing to capture stage bytes -- already $captured encoded " +
              s"bytes across the driver for this stage, over the ${budget}-byte budget remaining " +
              s"(maxCaptureBytes=$maxCaptureBytes, maxSessionCaptureBytes=$maxSessionCaptureBytes, " +
              s"already captured this session=${sessionCapturedBytes.get()}). Raise them " +
              "deliberately if the driver really has the heap for it.")
        }
        out(partitionId) = bytes
      })
    // A partition Spark's own RDD produced zero tasks/output for (a genuinely empty partition,
    // not a bug) still needs a real, present (if empty) entry -- readPartition's own contract
    // forbids treating a missing entry as "no data" ambiguously.
    for (i <- out.indices if out(i) == null) out(i) = Array.emptyByteArray
    sessionCapturedBytes.addAndGet(captured)
    out
  }

  /** How many encoded bytes `captureRealPartitionBytes` may allocate for this stage, or a throw
    * naming which precondition refused it. The session budget is checked here, BEFORE a job is
    * submitted, as well as being folded into the running per-partition check -- a listener that has
    * already spent its whole budget should not submit a job at all. */
  private def captureBudget(s: CapturedStage): Long = {
    if (s.bytesByPartition.isEmpty && s.numMappers > 0) {
      throw new IllegalStateException(
        "StageCaptureListener: refusing to capture stage bytes -- Spark reported no " +
          s"MapOutputStatistics for a stage with ${s.numMappers} mappers, so its size is unknown " +
          "and no ceiling can be enforced against it")
    }
    val remaining = maxSessionCaptureBytes - sessionCapturedBytes.get()
    if (remaining <= 0L) {
      throw new IllegalStateException(
        s"StageCaptureListener: refusing to capture stage bytes -- this listener has already pulled " +
          s"${sessionCapturedBytes.get()} bytes through the driver, at or over its whole-session " +
          s"ceiling of $maxSessionCaptureBytes. Every later query in this session degrades to " +
          "identity-and-stats-only from here on; raise maxSessionCaptureBytes deliberately, or use " +
          "a fresh listener per resumable query rather than one for the session.")
    }
    // Free and already recorded, so worth consulting first -- but a LOWER bound on what will
    // actually be allocated (see this class's `captureRealPartitionBytes` doc), which is why it can
    // only ever refuse early, never approve.
    val compressedBytes = s.bytesByPartition.sum
    if (compressedBytes > maxCaptureBytes) {
      throw new IllegalStateException(
        s"StageCaptureListener: refusing to capture stage bytes -- Spark's own statistics already " +
          s"report $compressedBytes compressed shuffle bytes across ${s.numPartitions} partitions, " +
          s"over the ${maxCaptureBytes}-byte ceiling this listener collects through the driver " +
          "(and the encoded form is larger still). Raise maxCaptureBytes deliberately if the driver " +
          "really has the heap for it.")
    }
    math.min(maxCaptureBytes, remaining)
  }

  private def countRows(partitionBytes: Array[Array[Byte]]): Long =
    partitionBytes.map(RowBytesCodec.countRows).sum
}

object StageCaptureListener {

  /** Default ceiling on how many ENCODED bytes of ONE stage's output this listener will pull
    * through the driver (see `captureRealPartitionBytes`). 256 MiB: comfortably above the fixtures
    * and small real stages this capture path is useful for, comfortably below the default driver
    * heap, and a deliberate refusal rather than a silent OOM for anything larger. Not a tuning knob
    * for throughput -- a stage genuinely too big to collect wants a producer-side capture path that
    * never funnels bytes through the driver at all, which is a different design, not a bigger
    * number here. */
  val DefaultMaxCaptureBytes: Long = 256L * 1024 * 1024

  /** Default ceiling on the TOTAL encoded bytes this listener will pull through the driver over its
    * whole lifetime. A per-stage ceiling alone caps one sample of an unbounded series: `onSuccess`
    * fires for every successful query in the session, each one may hand another `maxCaptureBytes`
    * worth of arrays to `ExchangeStore.store`, and nothing in this project evicts them -- with
    * `InMemoryExchangeStore` they are retained on the driver heap for the session's lifetime, and
    * even a durable backend accumulates slot directories nothing reclaims. 1 GiB (four full
    * per-stage captures) bounds that at something a driver can survive, after which every later
    * query degrades to identity-and-stats-only with a logged reason. A producer that genuinely
    * wants many resumable queries should use one listener per query (and [[close]] it), not one
    * listener with a bigger number. */
  val DefaultMaxSessionCaptureBytes: Long = 4L * DefaultMaxCaptureBytes

  /** Disclosed sentinel, not a real backend tag: no [[ExchangeStore]] is wired to per-stage
    * anchors in this phase, so there is no live handle to hold a real `handleKind` for. A caller
    * inspecting an `Anchor` with this `handleKind` must not attempt to deserialize its
    * `handlePayload` through any `ExchangeStore` -- there is nothing to reattach. */
  val NoHandleKind: String = "none/identity-and-stats-only"

  /** The [[AnchorStore]] key per-stage anchors for a given whole-query `queryId` are stored
    * under -- see this class's doc comment for why it is deliberately not `queryId` itself. */
  def stageQueryId(queryId: String): String = s"$queryId::stage"
}
