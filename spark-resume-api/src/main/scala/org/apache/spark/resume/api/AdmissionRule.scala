package org.apache.spark.resume.api

/** Everything about the candidate stage an [[AdmissionRule]] might need, independent of any
  * specific engine. `anchor` is `None` when no anchor was found for `fingerprint` at all (a plain
  * miss, not yet a rule decision) -- most rules will simply reject when `anchor.isEmpty` and
  * evaluate their real logic only when it's populated, but the field is exposed unconditionally
  * so a rule that wants to reason about the miss itself (e.g. an observability-only rule counting
  * miss reasons) can. */
final case class AdmissionCandidate(
    queryId: String,
    fingerprint: String,
    anchor: Option[Anchor],
    stageInfo: StageInfo)

/** Minimal, engine-agnostic facts about the stage currently being considered for admission. */
final case class StageInfo(stageId: Int, numPartitions: Int)

/** The outcome of one [[AdmissionRule]]. */
sealed trait AdmissionVerdict
/** This rule has no objection; admission proceeds to the next rule (or succeeds, if this was the
  * last one). */
case object Admit extends AdmissionVerdict
/** This rule has a definite, known reason admission must not proceed. */
final case class Reject(reason: String) extends AdmissionVerdict
/** This rule cannot determine an answer (a dependency was unavailable, insufficient information)
  * -- treated identically to [[Reject]] for the purpose of the admission decision itself (fail-
  * closed, design invariant A-2), but recorded distinctly in the emitted decision/observability
  * event so an operator can tell "we know this must not resume" apart from "we don't know," which
  * usually points at an operational problem (a store being unreachable) rather than a correctness
  * one. */
final case class Abstain(reason: String) extends AdmissionVerdict

/** One admission check in the chain [[org.apache.spark.resume.core.AdmissionEngine]] runs before
  * allowing a resume. Rules compose by conjunction: ALL registered rules must return [[Admit]]
  * for admission to proceed; the chain short-circuits and stops evaluating further rules on the
  * first non-`Admit` verdict (a later rule is never consulted after an earlier rejection, so a
  * rule with side effects cannot fire against an already-doomed candidate).
  *
  * A rule that throws is treated by the engine as [[Abstain]] with the exception's message as the
  * reason -- implementations are not required to catch their own exceptions, but SHOULD, if they
  * can produce a more specific reason than a bare stack trace would. */
trait AdmissionRule {
  def evaluate(candidate: AdmissionCandidate): AdmissionVerdict
}
