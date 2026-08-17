package org.apache.spark.resume.core

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.api._

class AdmissionEngineSpec extends AnyFunSuite with Matchers {

  private def anchor(fingerprint: String): Anchor =
    Anchor("1", "q1", 1L, fingerprint, "test", fingerprint.getBytes("UTF-8"), 1, 1, Array(1L), 1L,
      System.currentTimeMillis())

  private def candidate(hasAnchor: Boolean): AdmissionCandidate =
    AdmissionCandidate(
      queryId = "q1",
      fingerprint = "fp",
      anchor = if (hasAnchor) Some(anchor("fp")) else None,
      stageInfo = StageInfo(stageId = 7, numPartitions = 4))

  /** A rule whose verdict and call-count are both externally observable, so tests can assert
    * short-circuit behavior (a later rule not being consulted), not just the final outcome. */
  private class CountingRule(verdict: AdmissionVerdict) extends AdmissionRule {
    val calls = new AtomicInteger(0)
    override def evaluate(candidate: AdmissionCandidate): AdmissionVerdict = {
      calls.incrementAndGet()
      verdict
    }
  }

  private class ThrowingRule(message: String) extends AdmissionRule {
    override def evaluate(candidate: AdmissionCandidate): AdmissionVerdict =
      throw new RuntimeException(message)
  }

  test("no anchor for the fingerprint short-circuits to rejected without consulting any rule") {
    val rule = new CountingRule(Admit)
    val decision = AdmissionEngine.decide(candidate(hasAnchor = false), Seq(rule))
    decision.outcome shouldBe RejectedBy(AdmissionEngine.NoAnchorRuleName, "no anchor stored for fingerprint fp")
    rule.calls.get() shouldBe 0
  }

  test("all rules admitting results in Admitted") {
    val r1 = new CountingRule(Admit)
    val r2 = new CountingRule(Admit)
    val decision = AdmissionEngine.decide(candidate(hasAnchor = true), Seq(r1, r2))
    decision.outcome shouldBe Admitted
    r1.calls.get() shouldBe 1
    r2.calls.get() shouldBe 1
  }

  test("a Reject from an early rule short-circuits: later rules are never consulted") {
    val r1 = new CountingRule(Admit)
    val rejecting = new CountingRule(Reject("stale digest"))
    val r3 = new CountingRule(Admit)
    val decision = AdmissionEngine.decide(candidate(hasAnchor = true), Seq(r1, rejecting, r3))
    decision.outcome shouldBe RejectedBy(rejecting.getClass.getName, "stale digest")
    r1.calls.get() shouldBe 1
    rejecting.calls.get() shouldBe 1
    r3.calls.get() shouldBe 0 // the whole point of short-circuiting: never reached
  }

  test("an Abstain from an early rule short-circuits exactly like Reject, but is distinguished in the outcome") {
    val r1 = new CountingRule(Admit)
    val abstaining = new CountingRule(Abstain("store unreachable"))
    val r3 = new CountingRule(Admit)
    val decision = AdmissionEngine.decide(candidate(hasAnchor = true), Seq(r1, abstaining, r3))
    decision.outcome shouldBe AbstainedBy(abstaining.getClass.getName, "store unreachable")
    r3.calls.get() shouldBe 0
  }

  test("a rule that THROWS is treated as Abstain -- no exception escapes decide, and later rules are not consulted") {
    val thrower = new ThrowingRule("boom")
    val r2 = new CountingRule(Admit)
    val decision = AdmissionEngine.decide(candidate(hasAnchor = true), Seq(thrower, r2))
    decision.outcome match {
      case AbstainedBy(ruleName, reason) =>
        ruleName shouldBe thrower.getClass.getName
        reason should include("boom")
      case other => fail(s"expected AbstainedBy, got $other")
    }
    r2.calls.get() shouldBe 0
  }

  test("an empty rule chain with an anchor present results in Admitted (no rules means no objections)") {
    val decision = AdmissionEngine.decide(candidate(hasAnchor = true), Seq.empty)
    decision.outcome shouldBe Admitted
  }

  test("the emitted decision carries the candidate's queryId, fingerprint, and stageId") {
    val decision = AdmissionEngine.decide(candidate(hasAnchor = true), Seq.empty)
    decision.queryId shouldBe "q1"
    decision.fingerprint shouldBe "fp"
    decision.stageId shouldBe 7
  }
}
