package org.apache.spark.resume.spark35

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.exchange.Exchange

import org.apache.spark.resume.api._
import org.apache.spark.resume.core.{AdmissionEngine, Admitted}

/** One partition of a [[SkippedShuffleRDD]] -- carries nothing but its own index, since
  * everything needed to read it (`handle`, the store factory) lives on the RDD itself, not per
  * partition. */
private[spark35] final class SkippedShufflePartition(idx: Int) extends Partition {
  override def index: Int = idx
}

/** The REAL execution-skip RDD -- not the spike's `ex.execute()` cheat (see
  * `AqeExecutionSkipSpikeSpec`'s doc comment for that PoC and exactly what it deliberately did NOT
  * prove). Each partition's `compute()` runs ON THE EXECUTOR that will consume it and reads ONLY
  * that partition's bytes via `ExchangeStore.readPartition` -- no shuffle bytes are ever funneled
  * through the driver, the same "no execution" property a real live shuffle read would have.
  *
  * `storeFactory` (not a live `ExchangeStore` instance) is what actually ships to executors: a
  * `Function0[ExchangeStore]` closure, e.g. `() => new FsExchangeStore(baseDir)`, that
  * reconstructs a fresh store per task. This is deliberate, not incidental -- a live store
  * instance may hold backend-internal, non-serializable state (open connections, wire handles;
  * see `ExchangeHandle`'s own doc comment on the same concern for handles), so shipping a
  * *recipe* to build one, rather than one itself, is the same boundary this project's `handleKind`
  * / `serializeHandle` split already enforces for handles, applied to stores. */
private[spark35] final class SkippedShuffleRDD(
    sc: SparkContext,
    handle: ExchangeHandle,
    storeFactory: () => ExchangeStore,
    numFields: Int,
    numPartitions: Int)
    extends RDD[InternalRow](sc, Nil) {

  override def getPartitions: Array[Partition] =
    Array.tabulate(numPartitions)(i => new SkippedShufflePartition(i))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val store = storeFactory()
    val bytes = store.readPartition(handle, split.index)
    RowBytesCodec.decode(bytes, numFields)
  }
}

/** The leaf replacement for a skipped `Exchange` -- structurally a `LeafExecNode` (`children`
  * empty), the exact shape `AqeExecutionSkipSpikeSpec`'s gate 2 proved
  * `AdaptiveSparkPlanExec.createQueryStages`'s generic branch treats as ALREADY materialized (zero
  * new query stages, zero task submission for the ORIGINAL exchange subtree). Declares the SAME
  * `output`/`outputPartitioning`/`outputOrdering` as the `Exchange` it replaces -- gate 2 also
  * proved that survives Spark's own `EnsureRequirements`/`ValidateSparkPlan` validation and
  * produces correct downstream results, not just "doesn't crash." */
private[spark35] final case class SkippedExchangeExec(
    handle: ExchangeHandle,
    storeFactory: () => ExchangeStore,
    override val output: Seq[Attribute],
    override val outputPartitioning: Partitioning,
    override val outputOrdering: Seq[SortOrder])
    extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] =
    new SkippedShuffleRDD(sparkContext, handle, storeFactory, output.length, outputPartitioning.numPartitions)
}

/** Registered via `SparkSessionExtensions.injectQueryStagePrepRule` (see this module's README for
  * the exact `withExtensions` call a resuming driver makes) -- the production version of
  * `AqeExecutionSkipSpikeSpec`'s spike rule, now making a REAL admission decision per `Exchange`
  * and substituting a REAL byte-reading [[SkippedExchangeExec]], not an eager-execute cheat.
  *
  * Reuses `StageAdmissionCheck`'s exact admission logic (same fingerprint, same `AdmissionEngine`
  * call) rather than re-deriving it -- the DECISION this rule makes for a given `Exchange` is
  * required to be identical to what `StageAdmissionCheck.check` would report for it, since both
  * exist to answer the same question ("does this stage's anchor admit?") at two different moments
  * (a caller inspecting decisions before running the query, vs. this rule actually acting on one
  * during AQE's own plan preparation).
  *
  * A stage is only ever substituted when ALL of the following hold, checked in this order, ANY
  * failure falling through to normal execution (never a partial/unsafe substitution):
  *   1. `AdmissionEngine.decide` reports `Admitted` for this `Exchange`'s digest.
  *   2. The matching anchor's `handleKind` equals `exchangeStore.handleKind` -- an anchor written
  *      by a DIFFERENT backend (or `StageCaptureListener`'s `NoHandleKind` sentinel) is not
  *      reattachable through THIS store, the same check `StageAdmissionCheck.isReattachable`
  *      documents.
  *   3. `exchangeStore.isFresh(handle)` -- backend-authoritative, same as `SafeReattach` requires.
  *   4. `exchangeStore.checkIdentityIsolation(handle) == IsolationOk` -- same A-6 guard
  *      `SafeReattach.attempt` enforces for `reattach`, applied here to `readPartition` since this
  *      path does not go through `SafeReattach` itself (see `ExchangeStore.readPartition`'s doc
  *      comment on why: `SafeReattach` returns one `ReattachResult`, not per-partition bytes, so a
  *      caller building a real skip path on `readPartition` is responsible for the SAME
  *      preconditions `SafeReattach` would otherwise enforce). */
final class ExecutionSkipRule(
    queryId: String,
    anchorStore: AnchorStore,
    exchangeStore: ExchangeStore,
    storeFactory: () => ExchangeStore,
    providers: Seq[SourceFingerprint],
    rules: Seq[AdmissionRule] = Seq.empty)
    extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Loaded ONCE per apply() call, not once per Exchange node -- a query with several shuffle
    // stages would otherwise re-fetch the same anchor list redundantly.
    val anchors = anchorStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))

    plan.transformUp {
      case ex: Exchange =>
        val digest = WholePlanFingerprint.compute(ex, providers)
        val anchor = anchors.find(_.fingerprint == digest)
        val candidate = AdmissionCandidate(
          queryId = queryId,
          fingerprint = digest,
          anchor = anchor,
          stageInfo = StageInfo(stageId = 0, numPartitions = anchor.map(_.numPartitions).getOrElse(0)))

        AdmissionEngine.decide(candidate, rules).outcome match {
          case Admitted =>
            anchor.filter(_.handleKind == exchangeStore.handleKind) match {
              case Some(a) =>
                val handle = exchangeStore.deserializeHandle(a.handlePayload)
                val safeToSkip =
                  try {
                    exchangeStore.isFresh(handle) && exchangeStore.checkIdentityIsolation(handle) == IsolationOk
                  } catch {
                    case scala.util.control.NonFatal(_) => false // A-3: never let a check here crash the query
                  }
                if (safeToSkip) {
                  SkippedExchangeExec(handle, storeFactory, ex.output, ex.outputPartitioning, ex.outputOrdering)
                } else {
                  ex // fails closed: normal execution, same as any other admission refusal
                }
              case None => ex // Admitted but not reattachable through THIS store -- normal execution
            }
          case _ => ex // not admitted -- normal execution
        }
      case other => other
    }
  }
}
