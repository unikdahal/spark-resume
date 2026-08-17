package org.apache.spark.resume.core

import scala.util.control.NonFatal

import org.apache.spark.resume.api._

/** The category an [[AdmissionDecision]] falls into -- deliberately mirrors
  * [[org.apache.spark.resume.api.AdmissionVerdict]]'s three-way split so a caller reading a
  * decision doesn't have to learn a second vocabulary for the same concept. */
sealed trait DecisionOutcome
case object Admitted extends DecisionOutcome
final case class RejectedBy(ruleName: String, reason: String) extends DecisionOutcome
final case class AbstainedBy(ruleName: String, reason: String) extends DecisionOutcome

/** One completed admission decision, suitable for emitting as a structured observability event
  * (design doc §11) -- `outcome` alone tells a caller whether to proceed with
  * [[SafeReattach.attempt]]; `RejectedBy` vs `AbstainedBy` additionally tells an operator whether
  * this was a definite correctness call or a "we couldn't tell" -- the latter usually points at
  * an operational problem (a rule's own dependency being unavailable), not a correctness one. */
final case class AdmissionDecision(
    queryId: String,
    fingerprint: String,
    stageId: Int,
    outcome: DecisionOutcome,
    decidedAtMs: Long = System.currentTimeMillis())

/** Runs the ordered [[AdmissionRule]] chain against one [[AdmissionCandidate]] and produces a
  * single [[AdmissionDecision]]. Stateless and side-effect-free by itself -- every fail-closed
  * property below (design invariant A-2, docs/DESIGN.md §7) is enforced HERE, once, so no caller
  * has to re-derive it:
  *
  *   - No anchor at all for this fingerprint short-circuits to rejected before any rule runs
  *     (there is nothing to admit; running arbitrary rules against `None` would only invite a
  *     rule author to handle that case inconsistently).
  *   - Rules run strictly in order; the chain stops at the FIRST non-[[Admit]] verdict -- a later
  *     rule is never consulted after an earlier rejection, so a rule with side effects cannot
  *     fire against an already-doomed candidate.
  *   - A rule that THROWS is treated exactly as if it had returned
  *     `Abstain(exception.toString)` -- no exception ever escapes `decide`, and an admission
  *     decision can never fail open because a rule's own code had a bug.
  *   - [[Abstain]] and [[Reject]] both stop the chain and both result in "not admitted" -- they
  *     are distinguished only in the emitted [[AdmissionDecision]]'s `outcome`, for
  *     observability, never in whether the candidate proceeds. */
object AdmissionEngine {

  /** The name reserved for the engine's own built-in "no anchor at all" short-circuit below --
    * not a real [[AdmissionRule]] class, but every other `RejectedBy`/`AbstainedBy` carries a
    * real rule's class name, so this needs a name too rather than an empty string. */
  val NoAnchorRuleName: String = "<built-in: no anchor for this fingerprint>"

  def decide(candidate: AdmissionCandidate, rules: Seq[AdmissionRule]): AdmissionDecision = {
    val outcome =
      if (candidate.anchor.isEmpty) {
        RejectedBy(NoAnchorRuleName, s"no anchor stored for fingerprint ${candidate.fingerprint}")
      } else {
        runChain(candidate, rules)
      }
    AdmissionDecision(candidate.queryId, candidate.fingerprint, candidate.stageInfo.stageId, outcome)
  }

  private def runChain(candidate: AdmissionCandidate, rules: Seq[AdmissionRule]): DecisionOutcome = {
    val it = rules.iterator
    while (it.hasNext) {
      val rule = it.next()
      val verdict =
        try {
          rule.evaluate(candidate)
        } catch {
          case NonFatal(e) => Abstain(s"${rule.getClass.getName} threw: ${e.toString}")
        }
      verdict match {
        case Admit => // continue to the next rule
        case Reject(reason) => return RejectedBy(rule.getClass.getName, reason)
        case Abstain(reason) => return AbstainedBy(rule.getClass.getName, reason)
      }
    }
    Admitted
  }
}
