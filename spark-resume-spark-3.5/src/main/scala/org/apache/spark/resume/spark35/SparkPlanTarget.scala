package org.apache.spark.resume.spark35

import org.apache.spark.sql.execution.SparkPlan

import org.apache.spark.resume.api.FingerprintTarget

/** The concrete [[FingerprintTarget]] this integration module wraps every physical plan node in
  * before handing it to a registered `SourceFingerprint`. This is the one place `spark-resume-
  * api`'s Spark-free `FingerprintTarget` trait meets an actual `SparkPlan` -- every
  * `SourceFingerprint` implementation written against this module pattern-matches on `.plan`
  * after downcasting `target.node`, or (more conveniently) just matches on this case class
  * directly, as [[FileSourceFingerprint]] does. */
final case class SparkPlanTarget(plan: SparkPlan) extends FingerprintTarget {
  override def node: AnyRef = plan
}
