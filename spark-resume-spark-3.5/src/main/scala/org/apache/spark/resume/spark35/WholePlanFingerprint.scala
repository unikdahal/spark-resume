package org.apache.spark.resume.spark35

import scala.util.control.NonFatal

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}

import org.apache.spark.resume.api.SourceFingerprint

/** Computes one whole-plan fingerprint by walking a physical plan tree in pre-order, hashing each
  * node's contribution to a running trace, and hashing the whole trace at the end -- a Tier 1
  * (whole-query, not per-stage) fingerprint, per docs/DESIGN.md sec 8's three-tier strategy: the
  * per-stage granularity Tier 2/3 would give requires an AQE extension point this Spark version
  * does not expose publicly.
  *
  * Two Spark-specific traps this walker exists to avoid, both real and both found the hard way
  * (see this module's test suite for the AQE-on/AQE-off stability check that catches a
  * regression in the first one):
  *
  *   1. `AdaptiveSparkPlanExec` and `QueryStageExec` are BOTH `LeafExecNode` -- `.children` is
  *      always `Nil` on them. A walker that only recurses via `.children` silently stops at the
  *      wrapper and never reaches the real plan tree underneath, which would make every
  *      AQE-enabled query's fingerprint collapse to the same few bytes regardless of what the
  *      query actually does -- a false-positive-resumption hazard (invariant A-1), not a merely
  *      cosmetic bug. Both are explicitly unwrapped below before recursing.
  *   2. A leaf scan node with no matching registered `SourceFingerprint` must NOT fall back to a
  *      constant, non-discriminating fingerprint (a false-positive-resumption hazard again) --
  *      the fallback below still incorporates the node's own class name and `toString`, which is
  *      weaker than genuine content-awareness but strictly better than total non-discrimination
  *      (this is the same disclosed trade-off docs/DESIGN.md sec 4's lesson 1 and sec 7's A-1
  *      both call out: proven-safe for a recognized connector, "discriminates but isn't
  *      content-aware" for everything else, never "collides with everything"). */
object WholePlanFingerprint {

  /** @param providers tried, in order, against each leaf node -- the first one whose `supports`
    *   returns true wins; if none match, the disclosed fallback below is used instead. */
  def compute(root: SparkPlan, providers: Seq[SourceFingerprint]): String = {
    val sb = new StringBuilder
    walk(root, "0", sb, providers)
    Hashing.sha256Hex(sb.toString)
  }

  private def walk(plan: SparkPlan, path: String, sb: StringBuilder,
                    providers: Seq[SourceFingerprint]): Unit = plan match {
    // `adaptive.initialPlan`, deliberately NOT `adaptive.executedPlan` -- found by testing, not
    // predicted: `executedPlan` (== `currentPhysicalPlan`) is MUTATED IN PLACE as AQE actually
    // runs (partition coalescing inserts `AQEShuffleRead`, join strategies get re-picked from
    // runtime stats), so its value differs depending on whether/how far the query has executed
    // by the time this is called -- verified empirically (`explain()`'s own "Initial Plan" vs.
    // "Final Plan" sections literally differ in node shape for the identical query). A Tier 1
    // admission CHECK, by construction, runs before deciding whether to execute at all -- there
    // is no "how AQE would adapt it" to read yet. `initialPlan` is a `val`, fixed once at
    // construction and never mutated afterward (confirmed against Spark's own source, not
    // assumed), so it is the one plan shape available, unchanged, on BOTH sides: capture (after a
    // real run) and check (before one). This is a real, disclosed Tier 1 consequence, not a
    // shortcut: the fingerprint reflects the query's SHAPE, not how AQE happened to adapt it at
    // runtime -- exactly the "coarse-grained, whole-query" scope docs/DESIGN.md sec 14 states for
    // this phase. `QueryStageExec` nodes never appear in `initialPlan` at all (they are created
    // lazily as adaptive execution proceeds) -- that case is kept below only as a defensive
    // fallback for a caller who passes an already-materialized plan directly.
    case adaptive: AdaptiveSparkPlanExec =>
      walk(adaptive.initialPlan, path, sb, providers)
    case stage: QueryStageExec =>
      walk(stage.plan, path, sb, providers)
    case leaf if leaf.children.isEmpty =>
      sb.append(s"[$path]LEAF:${leafFingerprint(leaf, providers)}\n")
    case node =>
      sb.append(s"[$path]NODE:${node.getClass.getSimpleName}:${exprString(node)}\n")
      node.children.zipWithIndex.foreach { case (child, i) => walk(child, s"$path.$i", sb, providers) }
  }

  private def leafFingerprint(leaf: SparkPlan, providers: Seq[SourceFingerprint]): String = {
    val target = SparkPlanTarget(leaf)
    val matched = providers.find { p =>
      try p.supports(target) catch { case NonFatal(_) => false }
    }
    matched match {
      case Some(provider) =>
        try {
          s"${provider.getClass.getSimpleName}:${provider.fingerprint(target)}"
        } catch {
          // A-3: a provider's own bug must degrade this ONE leaf to the disclosed fallback, not
          // crash fingerprint computation for the whole query.
          case NonFatal(e) => degradedFallback(leaf, Some(e.toString))
        }
      case None => degradedFallback(leaf, None)
    }
  }

  /** Class name + `.expressions` + `.toString()`, not class name + `.expressions` alone -- found
    * necessary by testing, not predicted: a leaf's data-defining parameters are not always
    * Catalyst `Expression`s at all. `RangeExec`'s `start`/`end`/`step` are plain constructor
    * fields, invisible to `.expressions` (which only ever surfaces `Expression`-typed fields) --
    * two structurally different ranges collided on an identical fallback fingerprint until
    * `.toString()` was added, the exact false-positive-resumption shape invariant A-1 forbids,
    * for precisely the class of node this project has the LEAST specific knowledge of. Safe here
    * specifically because built-in Spark exec nodes are (with rare, disclosed exceptions) plain
    * case classes with structural, value-based `toString()` -- the identity-hashCode/toString
    * hazard this project's fingerprinting has warned about elsewhere applies to CONNECTOR-owned
    * scan nodes with custom equality, not Spark's own built-in operators. */
  private def degradedFallback(leaf: SparkPlan, failureReason: Option[String]): String = {
    val tag = failureReason.map(r => s"DEGRADED($r)").getOrElse("UNRECOGNIZED")
    // Same exprId-suffix strip as exprString, same reason (see that method's doc comment) --
    // `leaf.toString` for a built-in node with attribute output embeds the identical `#<exprId>`
    // pattern and would reintroduce the exact cross-session drift this fallback exists to avoid.
    val safeToString = ExprIdSuffix.replaceAllIn(leaf.toString, "")
    s"$tag:${leaf.getClass.getName}:${exprString(leaf)}:$safeToString"
  }

  // Matches Catalyst's `AttributeReference`/`ExprId` toString convention (`name#123L`, `none#123`
  // after .canonicalized renames the attribute) -- strips the trailing `#<digits>` exprId suffix
  // wherever it appears in the rendered expression string.
  private val ExprIdSuffix = """#\d+""".r

  /** `Expression.canonicalized`, found by testing (NOT by inspection) to still be unsafe for
    * this project's purpose, in a different and more surprising way than expected: reading
    * `AttributeReference.canonicalized`'s own Catalyst source shows it renames the attribute to
    * the literal string `"none"` -- but passes the SAME `exprId` straight through unchanged. Two
    * INDEPENDENT `SparkSession`s each assign exprIds from their own private, session-local
    * monotonic counter starting at 0, so the identical query plan built in two different sessions
    * gets two DIFFERENT sets of exprIds, and `.canonicalized.toString` still embeds them (`none
    * #14` vs. `none#30`) -- a same-plan, DIFFERENT-fingerprint bug that a single-session test
    * suite is structurally unable to catch (one session's counter is shared across every query in
    * that test), which is exactly why this project's cross-session stability tests exist and why
    * this was found by running them, not by reading `canonicalized`'s doc comment. `QueryPlan`-
    * level canonicalization (reassigning attributes to POSITION-based ids across a whole plan)
    * would fix this properly but risks silently canonicalizing away real structural differences
    * this project needs to keep discriminating; stripping the `#<exprId>` suffix textually is a
    * narrower, more auditable fix for the one specific leak this session's testing found. */
  private def exprString(plan: SparkPlan): String =
    plan.expressions.map(e => ExprIdSuffix.replaceAllIn(e.canonicalized.toString, "")).mkString(",")
}
