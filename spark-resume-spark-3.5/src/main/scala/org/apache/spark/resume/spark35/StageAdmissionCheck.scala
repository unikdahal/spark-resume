package org.apache.spark.resume.spark35

import org.apache.spark.sql.execution.QueryExecution

import org.apache.spark.resume.api._
import org.apache.spark.resume.core.{AdmissionDecision, AdmissionEngine}

/** The check half of per-stage fingerprinting -- see [[StageFingerprint]] for the mechanism and
  * [[StageCaptureListener]] for the capture half. Given a plan a new driver is about to run,
  * finds every `Exchange` subtree, fingerprints each one the identical way capture did, and runs
  * each candidate through `spark-resume-core`'s `AdmissionEngine` independently -- ONE
  * [[AdmissionDecision]] per stage found, not one for the whole query (contrast [[AdmissionCheck]]).
  * Pure and side-effect-free; a query with no shuffle stages at all yields an empty result, not an
  * error. */
object StageAdmissionCheck {

  /** One stage candidate's admission outcome, tagged with the tree position it came from
    * (`path`, [[StageCandidate.path]]) purely for a caller's own observability/logging -- the
    * decision itself never depends on the path, only on the content digest. */
  final case class StageDecision(path: String, decision: AdmissionDecision)

  def check(
      qe: QueryExecution,
      queryId: String,
      anchorStore: AnchorStore,
      providers: Seq[SourceFingerprint],
      rules: Seq[AdmissionRule] = Seq.empty): Seq[StageDecision] = {
    val candidates = StageFingerprint.checkSideCandidates(qe.executedPlan, providers)
    if (candidates.isEmpty) {
      Seq.empty
    } else {
      val anchors = anchorStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))
      candidates.zipWithIndex.map { case (c, i) =>
        val anchor = anchors.find(_.fingerprint == c.digest)
        val numPartitions = anchor.map(_.numPartitions).getOrElse(0)
        val admissionCandidate = AdmissionCandidate(
          queryId = queryId,
          fingerprint = c.digest,
          anchor = anchor,
          stageInfo = StageInfo(stageId = i, numPartitions = numPartitions))
        StageDecision(c.path, AdmissionEngine.decide(admissionCandidate, rules))
      }
    }
  }
}
