package org.apache.spark.resume.api

/** Computes a stable identifier for one candidate unit of work, such that two fingerprints
  * computed from two independent executions are equal if and only if those executions would, if
  * both completed successfully, produce equivalent output -- CRITICALLY including reading the
  * same version/snapshot of any external data source the work reads, not merely the same
  * structural plan shape (design invariant A-1, docs/DESIGN.md §7 -- the single most important
  * correctness property in this project; a collision between two logically different executions
  * is a silent correctness bug, not just a missed optimization).
  *
  * Implementations are matched to a specific data-source connector on purpose -- this interface
  * is deliberately narrow so a connector author can ship a `SourceFingerprint` alongside their
  * connector without depending on anything beyond the [[FingerprintTarget]] shape their connector
  * introduces. The engine tries registered implementations in priority order via `supports` and
  * falls through to a coarser, non-Spark-specific, still-safe fallback if none match (invariant
  * A-3: fingerprinting must never crash the query) -- that fallback lives in the engine
  * integration layer, not here, since "coarser but still discriminating" is itself
  * engine/connector-specific. */
trait SourceFingerprint {

  /** True if this implementation knows how to fingerprint `target`. Must be a fast, side-effect-
    * free type check (a pattern match on `target.node`'s class) -- the engine may call this for
    * every registered provider on every candidate node, on a hot planning path. */
  def supports(target: FingerprintTarget): Boolean

  /** MUST be a pure function of `target`'s already-resolved, already-planned state. May read
    * already-cached connector metadata (e.g. a table's already-resolved current snapshot id) but
    * must not perform new I/O that could itself fail or block -- a fingerprint computation that
    * can fail must degrade via a caught exception at the call site (invariant A-3), not via this
    * method partially completing. Only called after `supports` has returned `true` for the same
    * `target`; implementations may assume that precondition and are not required to re-validate
    * the node's shape. */
  def fingerprint(target: FingerprintTarget): String
}
