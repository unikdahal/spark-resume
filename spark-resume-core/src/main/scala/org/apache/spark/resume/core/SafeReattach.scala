package org.apache.spark.resume.core

import org.apache.spark.resume.api._

/** The result of [[SafeReattach.attempt]] -- three outcomes, not a boolean, so a caller can
  * distinguish WHY a reattach did not happen (both matter for observability, and only one of
  * them, `RefusedStale`, is an expected/routine outcome rather than a hazard worth alerting on). */
sealed trait ReattachOutcome
final case class Reattached(result: ReattachResult) extends ReattachOutcome
/** `ExchangeStore.isFresh` said no -- routine: the data was superseded or expired since the
  * anchor was written. Callers should treat this the same as any other admission rejection, not
  * as an error. */
case object RefusedStale extends ReattachOutcome
/** `ExchangeStore.checkIdentityIsolation` returned [[IsolationConflict]] -- see that method's
  * doc comment on [[ExchangeStore]] for exactly what hazard this is standing in for. Worth
  * distinguishing from `RefusedStale` in observability: this usually means something about how
  * the resuming process was launched (a reused application identity) needs attention, not that
  * the anchor itself was bad. */
final case class RefusedIsolationConflict(reason: String) extends ReattachOutcome

/** The ONE enforced choke point through which this project ever calls
  * [[ExchangeStore.reattach]] -- exists specifically so design invariants A-5 (freshness is
  * backend-authoritative) and A-6 (identity isolation) are structurally impossible to skip by
  * accident, rather than being a convention every call site has to remember to follow. Every
  * driver-side integration in this project MUST call `reattach` (on any `ExchangeStore`) only
  * through here, never directly.
  *
  * The specific hazard A-6 exists for -- calling `reattach` without first checking
  * `checkIdentityIsolation` -- was found empirically, not designed for in the abstract: a real
  * prototype hit a silently-empty (not erroring) result from exactly this omission on an
  * unrelated test path. This function is this project's structural answer to that finding. */
object SafeReattach {

  def attempt(store: ExchangeStore, handle: ExchangeHandle): ReattachOutcome = {
    if (!store.isFresh(handle)) {
      RefusedStale
    } else {
      store.checkIdentityIsolation(handle) match {
        case IsolationConflict(reason) => RefusedIsolationConflict(reason)
        case IsolationOk => Reattached(store.reattach(handle))
      }
    }
  }
}
