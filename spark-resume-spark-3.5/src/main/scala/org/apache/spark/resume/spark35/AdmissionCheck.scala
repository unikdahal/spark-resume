package org.apache.spark.resume.spark35

import scala.util.control.NonFatal

import org.apache.spark.sql.execution.QueryExecution

import org.apache.spark.resume.api._
import org.apache.spark.resume.core.{AdmissionDecision, AdmissionEngine}

/** The check half of Tier 1 (see [[SparkResumeListener]] for the capture half): given a plan a
  * NEW driver is about to run, fingerprint it the identical way capture did, look up whatever
  * anchor exists for that fingerprint, and run it through `spark-resume-core`'s
  * `AdmissionEngine`. Pure and side-effect-free -- callers decide what to do with the resulting
  * [[AdmissionDecision]] (Phase 1 has no execution-skip mechanism to act on it with; see this
  * module's README note and docs/DESIGN.md sec 14). */
object AdmissionCheck {

  def check(
      qe: QueryExecution,
      queryId: String,
      anchorStore: AnchorStore,
      providers: Seq[SourceFingerprint],
      rules: Seq[AdmissionRule] = Seq.empty): AdmissionDecision = {
    val plan = qe.executedPlan
    val fingerprint = WholePlanFingerprint.compute(plan, providers)
    val anchor = anchorStore.loadAnchors(queryId).find(_.fingerprint == fingerprint)
    val numPartitions =
      try math.max(1, plan.outputPartitioning.numPartitions) catch { case NonFatal(_) => 1 }
    val candidate = AdmissionCandidate(
      queryId = queryId,
      fingerprint = fingerprint,
      anchor = anchor,
      stageInfo = StageInfo(stageId = 0, numPartitions = numPartitions))
    AdmissionEngine.decide(candidate, rules)
  }
}
