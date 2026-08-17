package org.apache.spark.resume.api

/** The durable metadata-record store's contract -- three operations, deliberately minimal.
  * Distinct from [[ExchangeStore]], which holds the actual bytes/handle -- see docs/DESIGN.md §3.
  *
  * Implementations are expected to be usable from multiple concurrent driver processes (that is
  * the whole point: a crashed driver's writer and a new driver's reader/writer are different
  * processes by construction), so the fencing contract on `acquireGeneration`/`putAnchor` below
  * is load-bearing, not decorative -- see [[org.apache.spark.resume.api.testkit.AnchorStoreContract]]
  * for the concurrency test any implementation is expected to pass. */
trait AnchorStore {

  /** A monotonic fence for `queryId`: every write for this query must be gated on the value
    * returned here, and [[putAnchor]] must refuse a write for any generation other than the
    * CURRENT one at write time. Two concurrent callers for the same `queryId` must never receive
    * the same generation value -- implementations must make this call atomic with respect to
    * itself (a real CAS/lock primitive on the backend, not a read-then-write race). */
  def acquireGeneration(queryId: String): Long

  /** Refused (returns `false`, writes nothing) if `generation` is not the CURRENT generation for
    * `anchor.queryId` at the moment this call is evaluated on the backend -- so a writer whose
    * generation has since been superseded (a zombie driver still running past a new driver having
    * already called `acquireGeneration`) can never corrupt a later writer's anchors, and a caller
    * can tell a refusal apart from a transient backend failure only by this boolean, not an
    * exception: a refusal here is an expected, correct outcome of fencing working as designed.
    *
    * UNSPECIFIED, deliberately, and found to matter in practice: whether a SECOND `putAnchor` call
    * for the same `queryId` AND the same `generation` AND the same `anchor.fingerprint` (a real
    * scenario -- two structurally identical stages in one query, see
    * `spark-resume-spark-3.5`'s `StageFingerprintSpec`) results in ONE anchor or TWO. An
    * implementation may dedupe (`InMemoryAnchorStore` does, overwriting the first write) or keep
    * both (a store that appends, e.g. `RedisAnchorStore`'s Redis `RPUSH`) -- both are conforming.
    * `loadAnchors(queryId).size` is therefore NOT a store-independent observable a caller (or a
    * future test) should assert an exact value for when duplicate fingerprints are possible;
    * `AdmissionEngine` only ever needs ONE matching anchor to admit, so this ambiguity is harmless
    * for this project's own purposes, but a caller counting anchors for anything else should know
    * it varies by backend. */
  def putAnchor(generation: Long, anchor: Anchor): Boolean

  /** All anchors currently stored for `queryId`, in no particular order. Returns an empty
    * sequence (never throws) if none exist -- "no anchors for this query" is a normal, expected
    * state (a first run, or a query this store has never seen), not an error. */
  def loadAnchors(queryId: String): Seq[Anchor]
}
