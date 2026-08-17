package org.apache.spark.resume.api

/** Per-partition statistics from a live re-attach, in the shape a Spark integration layer needs
  * to repopulate whatever optimizer-visible runtime-statistics channel(s) a live execution of the
  * resumed node would have populated (design invariant A-4 -- see docs/DESIGN.md §7). Deliberately
  * flat data, not a Spark type: this module has no Spark dependency.
  *
  * `mapperAttempts` mirrors the producing run's actual per-mapper commit attempt ids, captured
  * verbatim rather than synthesized -- a backend whose read path filters by attempt id (skipping
  * a superseded speculative/retry attempt) would silently drop that mapper's real data under an
  * all-zero placeholder. */
final case class ReattachResult(
    numMappers: Int,
    numPartitions: Int,
    bytesByPartition: Array[Long],
    rowCount: Long,
    mapperAttempts: Array[Int])

/** The result of [[ExchangeStore.checkIdentityIsolation]] -- see that method's doc comment for
  * why this check exists and what silently breaks if a caller skips it. */
sealed trait IsolationResult
case object IsolationOk extends IsolationResult
final case class IsolationConflict(reason: String) extends IsolationResult

/** The pluggable backend holding the actual shuffle bytes (or wherever a backend equivalent of
  * "shuffle bytes" lives) and everything needed to safely re-attach to them from a new process.
  * Distinct from [[AnchorStore]], which holds only the lightweight metadata record -- see
  * docs/DESIGN.md §3 for why the split matters.
  *
  * Every method here is expected to be called from driver-side code, synchronously, at admission
  * time -- implementations should treat these as request/response RPCs to their backend, not as
  * something to buffer or batch internally (a caller wanting batched fan-out composes that on top
  * of this interface, e.g. calling `isFresh` for many handles concurrently). */
trait ExchangeStore {

  /** The backend-specific tag [[Anchor.handleKind]] should carry for handles this store produces
    * -- used by tooling that stores/inspects anchors from multiple backends to route
    * `deserializeHandle` correctly without guessing. Implementations should return a short,
    * stable, backend-identifying string (e.g. `"celeborn"`), never something version-specific. */
  def handleKind: String

  /** The backend's own serialized form of `handle`, suitable for [[Anchor.handlePayload]]. Must
    * round-trip through `deserializeHandle` losslessly. Must not throw for any handle this same
    * implementation produced. */
  def serializeHandle(handle: ExchangeHandle): Array[Byte]

  /** The inverse of `serializeHandle`. Implementations should throw a clear exception (not return
    * null or a handle that fails later) if `payload` was not produced by this same
    * implementation -- callers are expected to check [[Anchor.handleKind]] against `handleKind`
    * before calling this, but a defensive implementation should not silently accept a foreign
    * payload either. */
  def deserializeHandle(payload: Array[Byte]): ExchangeHandle

  /** True if the backend still considers `handle`'s data valid and unsuperseded. MUST be built
    * from the backend's own strongest available explicit versioning/generation/existence
    * primitive -- a length-only or mtime-only check is not an acceptable implementation of this
    * method for any backend that has something stronger available (design invariant A-5): a
    * same-size truncate-then-rewrite of the same slot is a real failure mode a naive
    * length-based check cannot distinguish from "still the original data." */
  def isFresh(handle: ExchangeHandle): Boolean

  /** Backend-specific application/producer-identity isolation guard. MUST be called, and MUST
    * return [[IsolationOk]], before any call to `reattach` on this handle -- see
    * [[org.apache.spark.resume.core.SafeReattach]], the enforced choke point this project ships
    * so a caller cannot accidentally skip this check.
    *
    * Why this exists, concretely: a resuming process that shares a backend-level
    * application/producer identity with the process that owns `handle`, while NOT actually
    * resuming from it (a fingerprint miss, a config change, an expired anchor -- any case where
    * this process is about to do a normal, independent recompute instead), can collide with that
    * producer's already-materialized state in some backends. The observed failure shape is not an
    * error -- it is a SILENT, WRONG, EMPTY (or truncated) result on the independent recompute,
    * because the backend treats the new write as somehow continuing the old one instead of
    * starting fresh. This was found empirically, not anticipated: building a test for an
    * unrelated property (fingerprint discrimination against a mutated source) hit exactly this
    * collision on its "correctly did not resume, must recompute independently" path. An
    * implementation whose backend has this hazard MUST detect the conflicting identity reuse here
    * and return [[IsolationConflict]] with a clear reason; an implementation whose backend has no
    * such hazard (each producer/consumer pairing is naturally isolated) can always return
    * [[IsolationOk]], but must say so deliberately rather than by having an empty/no-op
    * implementation that looks the same as one that forgot to check anything. */
  def checkIdentityIsolation(handle: ExchangeHandle): IsolationResult

  /** Re-attach the current process to `handle`'s data as if this process had produced it.
    * Callers MUST have already obtained [[IsolationOk]] from `checkIdentityIsolation` for this
    * exact handle in this exact process before calling this -- implementations are entitled to
    * assume that precondition holds and are not required to re-check it, which is exactly why
    * [[org.apache.spark.resume.core.SafeReattach]] exists as the one path callers should use
    * instead of calling this directly. */
  def reattach(handle: ExchangeHandle): ReattachResult

  /** PRODUCER-side: durably store `partitions` (one opaque byte blob per output partition, in
    * partition-index order -- what those bytes MEAN is entirely a Spark-integration-layer concern;
    * this trait has no Spark dependency and never interprets them) and return a handle a resuming
    * process can later read the SAME bytes back through via [[readPartition]].
    *
    * Added later than the rest of this trait, deliberately: every other method here models only
    * the RESUMING driver's operations (see this trait's own doc comment on why -- `handle`s are
    * backend-owned, opaque, never producer-constructed by a caller). `store`/`readPartition` are
    * the missing producer-side half, without which no `ExchangeStore` could ever be the thing a
    * resuming driver actually reattaches real data through -- see
    * `spark-resume-integration`'s `ProcessA`, which had to hand-build a `CelebornHandle` itself
    * specifically because `CelebornExchangeStore` had no such method to call, and
    * `spark-resume-spark-3.5`'s `StageCaptureListener`, which is correct to always write the
    * `NoHandleKind` sentinel for exactly the same reason (docs/DESIGN.md §6 records this as a
    * deliberate, later SPI addition, not an oversight in the original design).
    *
    * An implementation whose backend cannot support this at all (the same class of Tier 3 gap
    * `reattach` already has a documented escape hatch for -- see
    * `spark-resume-celeborn`'s `CelebornExchangeStore`, whose vanilla-Celeborn-client-API gap
    * applies here identically, since writing under a foreign application identity has the exact
    * same capability requirement as reading under one) MUST throw `UnsupportedOperationException`
    * naming the gap explicitly, the same as `reattach` does -- never silently drop the data or
    * return a handle that will fail later. No `storeSupported`-style testkit opt-out exists for
    * this method the way `reattachSupported` does for `reattach`: unlike reattach (where a
    * resuming driver calling a Tier-3-gapped backend is a normal, expected, routine outcome this
    * project structurally handles via `SafeReattach`'s `RefusedUnsupported`), a PRODUCER unable to
    * store its own output at all means that backend can never back a real anchor for anyone, which
    * is a fact worth a loud, explicit exception at the one call site that would ever invoke it,
    * not a routine, silently-tolerated outcome. */
  def store(partitions: Array[Array[Byte]]): ExchangeHandle

  /** The inverse of [[store]]: the exact bytes written for partition `partitionId` of `handle`.
    * Callers MUST have already obtained [[IsolationOk]] from `checkIdentityIsolation` AND
    * confirmed [[isFresh]] for this exact handle, the same precondition [[reattach]] has -- this
    * method is not routed through `SafeReattach` itself (that choke point returns one
    * `ReattachResult`, not per-partition bytes), so a caller building a real execution-skip path
    * on top of this method is responsible for enforcing that precondition itself, the same way
    * `SafeReattach.attempt` does for `reattach`.
    *
    * `partitionId` out of range for `handle`, or otherwise unreadable, MUST throw rather than
    * return an empty/zero-length result that could be silently misread as "this partition really
    * has no data" -- the same fail-loud posture [[reattach]]'s own doc comment requires. */
  def readPartition(handle: ExchangeHandle, partitionId: Int): Array[Byte]
}
