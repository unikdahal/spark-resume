package org.apache.spark.resume.spark35

import scala.util.control.NonFatal

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeLike}

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
  * `AqeExecutionSkipSpikeSpec`'s spike rule, now making a REAL admission decision per shuffle
  * exchange and substituting a REAL byte-reading [[SkippedExchangeExec]], not an eager-execute
  * cheat.
  *
  * Reuses `StageAdmissionCheck`'s exact admission logic (same fingerprint, same `AdmissionEngine`
  * call) rather than re-deriving it -- the VERDICT this rule reaches for a given exchange is
  * required to be identical to what `StageAdmissionCheck.check` would report for it, since both
  * exist to answer the same question ("does this stage's anchor admit?") at two different moments
  * (a caller inspecting decisions before running the query, vs. this rule actually acting on one
  * during AQE's own plan preparation).
  *
  * ==Scoped to `ShuffleExchangeLike`, not `Exchange`==
  * Deliberately the SAME node type the capture side is scoped to: `StageFingerprint.capturedStages`
  * only ever writes anchors for `ShuffleQueryStageExec`, because a broadcast exchange's output is
  * an in-memory broadcast variable, not shuffle-service-addressable bytes (see that object's doc
  * comment). Matching plain `Exchange` here made the two sides disagree by construction and left
  * the difference resting on fingerprints never colliding: a `BroadcastExchangeExec` that ever
  * resolved to an anchor would be replaced by [[SkippedExchangeExec]], which implements `doExecute`
  * only, so the downstream `BroadcastHashJoinExec`'s `executeBroadcast()` would hit `SparkPlan`'s
  * default `doExecuteBroadcast` and throw `UnsupportedOperationException` mid-query -- an A-3
  * violation reachable without any bug on this side. Narrowing the match makes that
  * unrepresentable instead of merely unlikely.
  *
  * ==Applied top-down, not bottom-up==
  * `transformDown`, deliberately: the digest for an exchange is computed from the subtree rooted at
  * it, so a bottom-up walk would fingerprint every exchange AFTER its own children had already been
  * substituted -- an outer exchange above a skipped one would hash a tree containing
  * `SkippedExchangeExec` (an unrecognized leaf, hashed via the fallback) rather than the original
  * subtree its anchor was captured from, never match, and silently re-execute. Only the innermost
  * exchange of a multi-shuffle query could ever be skipped. Top-down also skips MORE: when an outer
  * exchange is substituted, its whole subtree (including any inner exchange) disappears with it,
  * which is exactly right -- those bytes are already subsumed by the outer stage's stored output.
  *
  * `StageInfo.stageId` is the one field that deliberately does NOT correspond between this rule and
  * `StageAdmissionCheck`, and cannot: that object numbers candidates by pre-order position in
  * `AdaptiveSparkPlanExec.initialPlan` (the whole query, once, before it runs), while this rule is
  * handed whatever plan fragment AQE's own preparation passes it, repeatedly, as stages are carved
  * off -- there is no shared coordinate system to agree on. It is numbered here by pre-order
  * position within the fragment this invocation actually sees, which at least makes several
  * exchanges in one fragment distinguishable from each other (they were all `0` before Phase 4's
  * follow-up hardening). A node the id map has no entry for reports
  * [[ExecutionSkipRule.UnknownStageId]] (`-1`), never `0`: `0` is a REAL id belonging to the first
  * exchange in the fragment, so defaulting to it would alias an unknown stage onto a real one --
  * silently undoing the distinguishability this numbering exists for. A rule that must key on
  * stable stage identity should use `fingerprint`, which IS the same on both sides by construction.
  *
  * ==A-3: this rule must never fail the query it is preparing==
  * Registered via `injectQueryStagePrepRule`, it runs during AQE preparation of EVERY query in the
  * session -- including queries with no `Exchange` at all, and queries that have nothing to do with
  * resumption. Every fallible operation it performs (loading anchors from a possibly-unreachable
  * `AnchorStore`, computing fingerprints through caller-supplied `SourceFingerprint`s,
  * deserializing a possibly-corrupt handle payload, and the backend freshness/isolation checks) is
  * therefore inside the guard in `apply`, which degrades to the unmodified plan -- normal
  * execution -- rather than propagating. An earlier version guarded only the freshness/isolation
  * pair, leaving an `AnchorStore` outage or one corrupt stored payload able to kill an unrelated
  * user query outright.
  *
  * A stage is only ever substituted when ALL of the following hold, checked in this order, ANY
  * failure falling through to normal execution (never a partial/unsafe substitution):
  *   1. `AdmissionEngine.decide` reports `Admitted` for this exchange's digest.
  *   2. The matching anchor's `handleKind` equals `exchangeStore.handleKind` -- an anchor written
  *      by a DIFFERENT backend (or `StageCaptureListener`'s `NoHandleKind` sentinel) is not
  *      reattachable through THIS store, the same check `StageAdmissionCheck.isReattachable`
  *      documents.
  *   3. The anchor's `numPartitions` equals `ex.outputPartitioning.numPartitions`. Checked because
  *      the substitute is built from the PLAN's count while the stored bytes have the PRODUCING
  *      run's: a disagreement of even one would make `SkippedShuffleRDD.compute` ask the store for
  *      a partition index nobody ever wrote, which fails on the EXECUTOR (an
  *      `IndexOutOfBoundsException`/`NoSuchElementException` killing the user's query) rather than
  *      falling through to normal execution the way every other refusal here does. The A-3 guard in
  *      `apply` covers driver-side planning only, so this one has to be a precondition, not a
  *      catch.
  *   4. `exchangeStore.isFresh(handle)` -- backend-authoritative, same as `SafeReattach` requires.
  *   5. `exchangeStore.checkIdentityIsolation(handle) == IsolationOk` -- same A-6 guard
  *      `SafeReattach.attempt` enforces for `reattach`, applied here to `readPartition` since this
  *      path does not go through `SafeReattach` itself (see `ExchangeStore.readPartition`'s doc
  *      comment on why: `SafeReattach` returns one `ReattachResult`, not per-partition bytes, so a
  *      caller building a real skip path on `readPartition` is responsible for the SAME
  *      preconditions `SafeReattach` would otherwise enforce). */
final class ExecutionSkipRule(
    queryId: String,
    anchorStore: AnchorStore,
    storeFactory: () => ExchangeStore,
    providers: Seq[SourceFingerprint],
    rules: Seq[AdmissionRule] = Seq.empty)
    extends Rule[SparkPlan] with Logging {

  /** The driver-side store every check below runs against -- built FROM `storeFactory`, never
    * passed in alongside it. An earlier version took both a live `exchangeStore` and an
    * independent `storeFactory`, which let the two address different backends: `handleKind`
    * matching, `deserializeHandle`, `isFresh` and `checkIdentityIsolation` all validated one
    * store while `SkippedShuffleRDD.compute` read the actual bytes from the other, so a caller
    * whose two arguments disagreed got a green light from every safety check on one backend and
    * was then silently served partition bytes from another. Deriving it here makes that
    * mismatch unrepresentable. `lazy` so a session that never plans an `Exchange` never
    * constructs a store at all. */
  private lazy val exchangeStore: ExchangeStore = storeFactory()

  override def apply(plan: SparkPlan): SparkPlan =
    // A-3 (see class doc): this rule runs inside AQE's preparation of every query in the session.
    // Nothing it does -- including reaching an external AnchorStore over the network -- may turn
    // an unrelated user query into a failure. Any escape degrades to the unmodified plan, which
    // is exactly normal execution.
    try substitute(plan)
    catch {
      case NonFatal(e) =>
        // Silent to the QUERY, never to the OPERATOR. An unreachable AnchorStore, a wrong store
        // base dir, or a bug in a caller-supplied SourceFingerprint makes this throw on every
        // query in the session, and returning the plan unchanged is indistinguishable from an
        // ordinary fingerprint miss: full-cost execution, no error, no warning, nothing to tell
        // the two apart short of re-running with a fixed config. Logged at WARN so a permanently
        // disabled skip path is discoverable rather than merely invisible.
        logWarning(
          s"spark-resume: execution-skip rule degraded to normal execution for queryId=$queryId " +
            "-- this is a refusal to skip, not a query failure, but nothing will ever be skipped " +
            "until the cause is fixed", e)
        plan
    }

  private def substitute(plan: SparkPlan): SparkPlan = {
    // Short-circuit BEFORE touching the anchor store: a plan with no Exchange has nothing this
    // rule could ever substitute, so it must not pay for (or be able to fail on) an anchor fetch.
    // `StageAdmissionCheck` guards its own `loadAnchors` the same way, for the same reason.
    if (!containsExchange(plan)) {
      return plan
    }
    // Loaded ONCE per apply() call, not once per Exchange node -- a query with several shuffle
    // stages would otherwise re-fetch the same anchor list redundantly.
    val anchors = anchorStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))
    val stageIds = preOrderExchangeIds(plan)

    // transformDown, not transformUp -- see the class doc: fingerprinting bottom-up would hash a
    // subtree this same walk had already mutated, so no exchange above a skipped one could ever
    // match its own anchor. Top-down also keeps every node this partial function is handed an
    // ORIGINAL node from `plan` (a substituted exchange is a leaf, so the walk never descends
    // through a rebuilt parent), which is what makes the identity-keyed `stageIds` lookup below
    // actually hit.
    plan.transformDown {
      case ex: ShuffleExchangeLike =>
        val digest = WholePlanFingerprint.compute(ex, providers)
        val anchor = anchors.find(_.fingerprint == digest)
        val candidate = AdmissionCandidate(
          queryId = queryId,
          fingerprint = digest,
          anchor = anchor,
          stageInfo = StageInfo(
            stageId = Option(stageIds.get(ex)).map(_.intValue).getOrElse(ExecutionSkipRule.UnknownStageId),
            numPartitions = anchor.map(_.numPartitions).getOrElse(0)))

        AdmissionEngine.decide(candidate, rules).outcome match {
          case Admitted =>
            anchor.filter(_.handleKind == exchangeStore.handleKind) match {
              case Some(a) =>
                // deserializeHandle is INSIDE this guard deliberately: its own SPI contract
                // requires it to throw on a payload it did not produce, and a stored payload can
                // be corrupt/truncated/foreign while still carrying a matching handleKind. Before
                // Phase 4's follow-up hardening it sat outside, so that one case failed OPEN into
                // the user's query instead of closed to normal execution like every other branch.
                val safeToSkip =
                  try {
                    val handle = exchangeStore.deserializeHandle(a.handlePayload)
                    // The partition-count agreement check is FIRST, and is a precondition rather
                    // than something the catch below could ever contain: a mismatch would not fail
                    // here at all, it would fail later, on the EXECUTOR, inside
                    // SkippedShuffleRDD.compute asking the store for a partition index nobody
                    // wrote -- past the point where falling through to normal execution is still
                    // possible. See the class doc's condition 3.
                    if (a.numPartitions == ex.outputPartitioning.numPartitions &&
                        exchangeStore.isFresh(handle) &&
                        exchangeStore.checkIdentityIsolation(handle) == IsolationOk) {
                      Some(handle)
                    } else {
                      None
                    }
                  } catch {
                    case NonFatal(_) => None // A-3: never let a check here crash the query
                  }
                safeToSkip match {
                  case Some(handle) =>
                    SkippedExchangeExec(handle, storeFactory, ex.output, ex.outputPartitioning, ex.outputOrdering)
                  case None =>
                    ex // fails closed: normal execution, same as any other admission refusal
                }
              case None => ex // Admitted but not reattachable through THIS store -- normal execution
            }
          case _ => ex // not admitted -- normal execution
        }
    }
  }

  /** Whether `plan` contains any `Exchange` this rule could substitute -- deliberately NOT
    * `plan.exists(_.isInstanceOf[Exchange])`, which walks `children` and would therefore hit the
    * exact trap `WholePlanFingerprint`'s doc comment warns about at length: `AdaptiveSparkPlanExec`
    * and `QueryStageExec` are BOTH `LeafExecNode`, so `children` is `Nil` on them and a
    * children-only walk silently reports "no exchanges" for a wrapped plan. Getting that wrong
    * here would not fail loudly -- it would just make the skip never fire, a functional regression
    * wearing an optimization's clothes.
    *
    * Deliberately a SUPERSET of what `transformDown` below can actually reach -- it descends into
    * the wrappers (`transformDown` cannot, for the same `children`-is-`Nil` reason) and it matches
    * every `Exchange`, not only the `ShuffleExchangeLike` subset the rule will act on. Erring that
    * way is the safe direction: a false positive costs one redundant anchor fetch, a false negative
    * would silently disable the feature. */
  private def containsExchange(plan: SparkPlan): Boolean = plan match {
    case _: Exchange => true
    case a: AdaptiveSparkPlanExec => containsExchange(a.initialPlan)
    case q: QueryStageExec => containsExchange(q.plan)
    case other => other.children.exists(containsExchange)
  }

  /** Pre-order position of each `ShuffleExchangeLike` in `plan` -- the same node type the rule
    * itself matches, so every id it hands out belongs to a node that was actually a candidate.
    * Keyed by REFERENCE (`IdentityHashMap`, not a plain `Map`): `SparkPlan` is a case class with
    * structural equality, so two textually identical exchanges in one plan would collide into a
    * single key and silently share an id. Reference keying only works because the walk below and
    * the `transformDown` above both see the ORIGINAL node instances (see that walk's comment). */
  private def preOrderExchangeIds(plan: SparkPlan): java.util.IdentityHashMap[SparkPlan, Integer] = {
    val ids = new java.util.IdentityHashMap[SparkPlan, Integer]()
    var next = 0
    def go(p: SparkPlan): Unit = {
      p match {
        case _: ShuffleExchangeLike =>
          ids.put(p, next)
          next += 1
        case _ =>
      }
      p.children.foreach(go)
    }
    go(plan)
    ids
  }
}

object ExecutionSkipRule {

  /** Reported as `StageInfo.stageId` when [[ExecutionSkipRule]] cannot place an exchange in the
    * fragment it is preparing. Deliberately `-1`, not `0`: `0` is a real id (the first exchange in
    * pre-order), so defaulting an unknown node to it would alias two distinct stages onto one id
    * for any `AdmissionRule` or observability consumer keying on `stageId` -- the exact
    * indistinguishability per-fragment numbering was introduced to remove. An unknown position is
    * a fact worth reporting as unknown. */
  val UnknownStageId: Int = -1
}
