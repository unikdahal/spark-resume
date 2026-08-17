package org.apache.spark.resume.api

/** An opaque wrapper around one candidate unit of work's engine-native representation (in a
  * Spark integration, a physical plan node -- but this module has no Spark dependency, so it
  * cannot say that directly). A `SourceFingerprint` implementation is written against a specific
  * engine-integration module's concrete `FingerprintTarget` subtype (e.g. a `spark-resume-spark-
  * 3.5`-defined `SparkPlanTarget(node: SparkPlan)`), never against this trait's own (empty)
  * surface -- `node` exists so an implementation can pattern-match on the concrete subtype it
  * knows how to handle and safely ignore ones it doesn't (see [[SourceFingerprint.supports]]).
  *
  * This exists specifically so [[SourceFingerprint]] can be typed against something more useful
  * than `Any` while `spark-resume-api` still has zero Spark dependency -- see docs/DESIGN.md §8
  * (the three-tier Spark integration strategy) for why that boundary matters. */
trait FingerprintTarget {

  /** The engine-native node this target wraps, as `AnyRef` -- an implementation downcasts this
    * (after `supports` has already confirmed it recognizes the shape) to read whatever
    * connector-specific state it needs. Not typed any more specifically than `AnyRef` here,
    * deliberately: doing so would require this module to depend on the engine type. */
  def node: AnyRef
}
