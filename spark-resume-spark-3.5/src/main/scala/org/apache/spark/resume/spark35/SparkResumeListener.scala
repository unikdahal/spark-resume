package org.apache.spark.resume.spark35

import scala.util.control.NonFatal

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.spark.resume.api._
import org.apache.spark.resume.api.memory.InMemoryExchangeStore

/** Captures one whole-query fingerprint per successful execution and writes it as an [[Anchor]]
  * -- the Tier 1 half of this project's capture path (docs/DESIGN.md sec 8). Built entirely on
  * `QueryExecutionListener`, a stable public API; register an instance via
  * `spark.listenerManager.register(...)`.
  *
  * Disclosed, not glossed over: `QueryExecutionListener.onSuccess` fires AFTER the whole query has
  * already finished, with no public hook into an individual stage's own materialization moment
  * (that is exactly the Tier 2 extension point docs/DESIGN.md sec 8 proposes and does not yet
  * have). Consequently the [[ReattachResult]] statistics captured here are honest placeholders,
  * not real per-partition byte measurements: `numMappers` is unknown at this layer and recorded
  * as `1`, `bytesByPartition` is unknown and recorded as zeros, and `numPartitions`/`rowCount` are
  * the two facts this layer CAN read from public state (the plan's own output partitioning, and
  * the root node's `numOutputRows` metric where present). This is exactly what docs/DESIGN.md
  * sec 14 means by Phase 1 proving the DECISION layer, not a real skip-execution path -- see
  * [[AdmissionCheck]], the other half, for where those decisions actually get made.
  *
  * A second, sharper trap found only by running the same capture-then-check test dozens of times
  * in a loop, not by inspection (it does not reproduce reliably enough for a single run to catch
  * it): `QueryExecutionListener` events are NOT delivered synchronously with the query call that
  * triggers them. `ExecutionListenerManager` posts through `SparkContext`'s own asynchronous
  * `LiveListenerBus` (confirmed against Spark's own source, `ExecutionListenerManager.init`
  * calls `sparkContext.listenerBus.addToSharedQueue(...)`), so `onSuccess` can still be
  * IN FLIGHT on a background dispatcher thread after `.collect()` has already returned to the
  * caller. A caller that immediately does something time-sensitive right after triggering the
  * captured query -- this project's own test suite unregistering the listener, or a real driver
  * shutting down -- can race the dispatch and silently lose the capture with no exception
  * anywhere (this listener's own `catch` is exactly why it stayed silent: A-3's crash-safety
  * swallows the symptom along with genuine bugs, which is why this needed a debug build with the
  * catch temporarily removed to actually see). There is no callback-based fix on this project's
  * side -- this module's own test suite synchronizes by calling
  * `spark.sparkContext.listenerBus.waitUntilEmpty(timeoutMs)` after triggering the query and
  * before relying on the capture having landed (see `AdmissionCheckIntegrationSpec`). CAVEAT:
  * `SparkContext.listenerBus` is `private[spark]` -- that call only compiles because this
  * module's own package (`org.apache.spark.resume.spark35`) sits inside `org.apache.spark`. A
  * caller outside that package tree has NO public synchronization point for this at all; that is
  * a real, disclosed Tier 1 limitation, not just an implementation detail of this listener. */
final class SparkResumeListener(
    queryId: String,
    anchorStore: AnchorStore,
    exchangeStore: InMemoryExchangeStore,
    providers: Seq[SourceFingerprint])
    extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    // Never let a capture-path bug fail or slow down the query it's observing -- this listener
    // runs on the driver's query-execution-completion path, not a background thread, so an
    // uncaught exception here would surface as this project breaking a caller's actual query.
    try {
      val plan = qe.executedPlan
      val fingerprint = WholePlanFingerprint.compute(plan, providers)
      val numPartitions = safeNumPartitions(plan)
      val rowCount = safeRowCount(plan)
      val placeholder = ReattachResult(
        numMappers = 1,
        numPartitions = numPartitions,
        bytesByPartition = Array.fill(numPartitions)(0L),
        rowCount = rowCount,
        mapperAttempts = Array(0))
      val handle = exchangeStore.put(fingerprint, placeholder)
      val payload = exchangeStore.serializeHandle(handle)
      val generation = anchorStore.acquireGeneration(queryId)
      val anchor = Anchor(
        schemaVersion = "1",
        queryId = queryId,
        generation = generation,
        fingerprint = fingerprint,
        handleKind = exchangeStore.handleKind,
        handlePayload = payload,
        numMappers = 1,
        numPartitions = numPartitions,
        bytesByPartition = Array.fill(numPartitions)(0L),
        rowCount = rowCount,
        createdAtMs = System.currentTimeMillis())
      anchorStore.putAnchor(generation, anchor)
    } catch {
      case NonFatal(_) => // A-3 applied to the integration layer: degrade silently, never crash.
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()

  private def safeNumPartitions(plan: org.apache.spark.sql.execution.SparkPlan): Int =
    try math.max(1, plan.outputPartitioning.numPartitions) catch { case NonFatal(_) => 1 }

  private def safeRowCount(plan: org.apache.spark.sql.execution.SparkPlan): Long =
    try {
      plan.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
    } catch { case NonFatal(_) => 0L }
}
