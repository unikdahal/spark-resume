# spark-resume-spark-3.5

Tier 1 Spark 3.5 integration (`docs/DESIGN.md` §8): fingerprinting a real physical plan and
driving `spark-resume-core`'s `AdmissionEngine` off it, built entirely on public Spark APIs.
Depends on a real, published, vanilla Apache Spark 3.5.9 — not a private fork — at `provided`
scope.

## What this proves

`AdmissionCheckIntegrationSpec` runs the actual proof: capture a real query's fingerprint in one
`SparkSession`, look it up from a completely independent second `SparkSession`, and get a correct
admission decision — `Admitted` for the identical query, `Rejected` for a structurally different
one, and (the go/no-go case) `Rejected` when the underlying file was mutated between capture and
check, not silently admitted. `StageAdmissionCheckIntegrationSpec` proves the same claim at
per-stage granularity (see below). 48 tests across the whole repo, `mvn clean install`, reproduced
clean across multiple consecutive full runs (see the async-listener-bus bug below for why rerun
count mattered here).

## Per-stage fingerprinting (`StageFingerprint` / `StageCaptureListener` / `StageAdmissionCheck`)

The Tier-1-only degradation of the Tier 2 extension point above: no public hook fires at a
stage's own materialization moment, so per-stage identity is recovered by fingerprinting the same
exchange subtree from both sides of the capture/check split and matching by CONTENT, not tree
position. See `StageFingerprint`'s doc comment for the full mechanism, `StageFingerprintSpec` for
the proof (a real shuffle query's check-side digest, computed before it ever runs, verified equal
to its own capture-side digest computed after it does), and `StageAdmissionCheckIntegrationSpec`
for the same end-to-end wiring `AdmissionCheckIntegrationSpec` proves for the whole-query path.
Per-stage anchors carry REAL runtime statistics (`numMappers`/`numPartitions`/`bytesByPartition`,
straight from `ShuffleExchangeLike` and `MapOutputStatistics`) rather than the whole-query
listener's placeholders — but are NOT reattachable in this phase (`Anchor.handleKind` is the
disclosed sentinel `StageCaptureListener.NoHandleKind`; no `ExchangeStore` is wired to them yet).

## What this does NOT prove

**No execution is ever skipped.** There is no public Spark 3.5 extension point that lets a
library intercept an individual stage's execution and substitute previously-computed output —
that is the Tier 2 extension point `docs/DESIGN.md` §8 proposes and does not yet have. What this
module proves is the *decision* layer: given a real plan, would admission fire, and why. Turning
an `Admitted` decision into an actual skipped stage needs Tier 2/3, which this phase has none of.

**Capture statistics are honest placeholders, not real measurements.** `SparkResumeListener`
fires from `QueryExecutionListener.onSuccess`, after the whole query has already finished, with
no hook into any individual stage's materialization moment. `numMappers`/`bytesByPartition` in
the anchors it writes are disclosed placeholders (`1` and all-zero respectively); `numPartitions`
and `rowCount` are the two facts genuinely readable from public post-execution state.

## Three real bugs found building and testing this, none anticipated by design

1. **`AdaptiveSparkPlanExec.executedPlan` (i.e. `currentPhysicalPlan`) is mutated in place as AQE
   actually runs** — it differs depending on how far execution has progressed when read. A Tier 1
   admission *check*, by construction, runs before deciding whether to execute at all, so there
   is no "how AQE would adapt it" to read yet on the check side. Fixed by walking
   `AdaptiveSparkPlanExec.initialPlan` instead — a `val`, fixed once at construction and never
   mutated afterward (confirmed against Spark's own source) — so capture (after a real run) and
   check (before one) read the same stable snapshot. A real, disclosed consequence: the
   fingerprint reflects the query's pre-adaptive SHAPE, not how AQE happened to adapt it at
   runtime; an AQE-enabled and a non-AQE-enabled run of the identical query are NOT expected to
   fingerprint identically (Spark's own plan-preparation pipeline genuinely differs between the
   two paths, confirmed by dumping both `explain()` outputs side by side, not assumed).
2. **`Expression.canonicalized` does not strip `exprId`.** `AttributeReference.canonicalized`
   renames the attribute to the literal string `"none"` but passes the SAME `exprId` straight
   through (confirmed by reading Catalyst's own source). Two INDEPENDENT `SparkSession`s each
   assign exprIds from their own private, session-local counter starting at 0, so the identical
   query plan built in two different sessions gets two different exprIds — `.canonicalized`
   alone still hashed them as different. Found by a cross-session stability test, which a
   single-session test suite is structurally unable to catch (one session's counter is shared
   across every query in that suite). Fixed by stripping the `#<exprId>` suffix textually from
   every rendered expression string.
3. **`QueryExecutionListener` events are delivered asynchronously**, through `SparkContext`'s own
   `LiveListenerBus` (confirmed against Spark's source: `ExecutionListenerManager` posts via
   `sparkContext.listenerBus.addToSharedQueue`), NOT synchronously with the query call that
   triggers them. A caller that does something time-sensitive immediately after triggering the
   captured query — this project's own first-draft test suite unregistering the listener right
   after `.collect()` — can race the dispatch and silently lose the capture, no exception
   anywhere. Reproduced intermittently (roughly 1 run in 3–4) across full-suite runs, never in a
   single isolated run of the failing test alone — found by looping the suite, not by a single
   pass. Fixed by calling `spark.sparkContext.listenerBus.waitUntilEmpty(timeoutMs)` after
   triggering the query and before relying on the capture having landed. See
   `SparkResumeListener`'s doc comment for the full account and
   `AdmissionCheckIntegrationSpec.captureAndWait` for the fix applied. CAVEAT, disclosed not
   glossed over: `SparkContext.listenerBus` is `private[spark]` — this fix only compiles because
   this module's package lives inside `org.apache.spark`. A caller outside that package tree has
   no public API to synchronize on at all; that gap is a real Tier 1 limitation.

A fourth, smaller finding: a leaf node's fallback fingerprint (for a scan type with no registered
`SourceFingerprint`) built only from class name + `.expressions` silently collided two
structurally different `RangeExec` plans (`range(0,10)` vs. `range(0,20)`) — `start`/`end`/`step`
are plain constructor fields, not Catalyst `Expression`s, so `.expressions` never surfaced them.
Fixed by folding the node's own `.toString()` into the fallback too.
