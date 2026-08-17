package org.apache.spark.resume.integration

import org.apache.spark.sql.{DataFrame, SparkSession}

/** The identical query both [[ProcessA]] and [[ProcessB]] build. Identical Scala source run in
  * two INDEPENDENT `SparkSession`s (in two independent JVMs, not just two sessions in one test
  * process) is exactly the scenario `spark-resume-spark-3.5`'s own cross-session
  * fingerprint-stability tests already prove is safe -- see `WholePlanFingerprint`'s exprId-
  * stripping doc comment for why two sessions' own private, independently-numbered `exprId`
  * counters would otherwise make an identical query fingerprint differently. This module composes
  * that already-proven property across a REAL OS process boundary rather than reproving it.
  *
  * `repartition(n)` (not an aggregation) is deliberate: the simplest shape guaranteed to produce
  * exactly one `Exchange`/`ShuffleQueryStageExec`, so this module's assertions ("exactly one
  * admitted, reattachable stage") don't have to account for AQE partition-coalescing collapsing
  * or splitting stage count in a way that would make this fixture's own shape part of what's
  * under test, rather than the composition this module actually exists to prove.
  *
  * @param numPartitions the repartition target -- part of the plan `Exchange`'s own partitioning
  *   spec, so a DIFFERENT value produces a DIFFERENT fingerprint (confirmed, not assumed --
  *   `spark-resume-spark-3.5`'s own exchange-collision tests already prove structurally distinct
  *   exchanges don't collide). `ProcessB`'s `miss` scenario deliberately passes a different value
  *   than `ProcessA` used, to prove a genuine, real fingerprint miss (not just an admitted match)
  *   also composes correctly across a real process boundary. */
object Fixture {
  def query(spark: SparkSession, numPartitions: Int = 3): DataFrame =
    spark.range(0, 4000, 1, 4).repartition(numPartitions).toDF()
}
