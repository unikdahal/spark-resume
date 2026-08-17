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
per-stage granularity (see below). See the repo root README for the current whole-repo test count
(it has grown substantially since this module was first built) — `mvn clean install`, reproduced
clean across multiple consecutive full runs (see the async-listener-bus bug below for why rerun
count mattered here).

## Checked for the same hole `spark-resume-iceberg` was found to have

`spark-resume-iceberg`'s `IcebergFingerprintProvider` was found to have a real, confirmed A-1 gap:
the same scan node returns a NEWER snapshot id if asked again post-execution after a commit lands
in between, because its underlying `Table` handle auto-refreshes live. `FileSourceFingerprint` was
checked against the identical scenario, not assumed safe by analogy — confirmed NOT vulnerable
(`FileSourceFingerprintSpec`'s "A-1 SAFETY" test): the same `FileSourceScanExec` node fingerprints
identically before and after an on-disk overwrite with no re-plan in between, because
`InMemoryFileIndex`'s file listing is fixed at plan time, not live-refreshing. Corroborated, not
just asserted: actually executing against the stale plan after such an overwrite fails outright
(parquet's own `FileNotFoundException`, since the exact part-files planned against are gone)
rather than silently reading the new data.

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
disclosed sentinel `StageCaptureListener.NoHandleKind`, enforced two ways: the convenience check
`StageAdmissionCheck.isReattachable` and, independently, a real `ExchangeStore`'s own
`deserializeHandle` refusing the sentinel payload outright — proven, not just asserted, in
`StageAdmissionCheckIntegrationSpec`).

Two things confirmed by deliberately forcing them, not left as untested reasoning: (1) a
join+aggregation query correctly yields 2 check-side candidates (the shuffle and the broadcast
exchange) but only 1 captured stage (`StageFingerprint` is deliberately scoped to
`ShuffleQueryStageExec`; the broadcast candidate legitimately has no match — see `docs/DESIGN.md`
§14 Phase 2 for what this implies about AQE re-planning above an already-materialized stage); (2)
two structurally identical exchanges over the same source really do produce the same digest (a
real, reproduced collision, forced via `spark.sql.exchange.reuse=false` so Spark's own
`ReuseExchange` rule doesn't merge them first) — reasoned to be benign and confirmed end to end
(`StageAdmissionCheckIntegrationSpec`), though which store implementation keeps one or both
same-digest anchors turned out to be implementation-specific (see `docs/DESIGN.md` §14).

## Real execution-skipping (`RowBytesCodec` / `ExecutionSkipRule` / `SkippedShuffleRDD`)

**Execution IS actually skipped now, for real, not just decided on paper** — the single biggest
change this README has ever needed. A prior version said no public Spark 3.5 extension point could
let a library intercept an individual stage's execution and substitute previously-computed output.
Checked again in Phase 4 and found wrong: `SparkSessionExtensions.injectQueryStagePrepRule`
(public since Spark 3.0) runs on the whole plan BEFORE any `Exchange` materializes — a disclosed
spike (`AqeExecutionSkipSpikeSpec`) proved a leaf substitute for an `Exchange` survives Spark's own
validation and produces correct downstream results, and `ExecutionSkipRule` is the REAL,
non-spike version: it makes the identical admission decision `StageAdmissionCheck` would (same
digest, same `AdmissionEngine` call), and when admitted through a reattachable handle, substitutes
`SkippedExchangeExec` — a leaf whose `doExecute()` returns a `SkippedShuffleRDD` that reads REAL
bytes per partition, ON THE EXECUTOR that will consume them, via `ExchangeStore.readPartition`.
No shuffle bytes are funneled through the driver; each partition's `compute()` runs independently.

`RowBytesCodec` is what makes the bytes real: Spark's own `UnsafeRow` binary format, length-prefixed
per row — the same shape Spark's shuffle write path uses internally — encoded by
`StageCaptureListener`'s new `exchangeStore` parameter (reading a captured stage's ACTUAL
materialized output via `stage.plan.execute()`, cheap since it re-reads already-computed shuffle
output, not a recomputation) and decoded the same way inside `SkippedShuffleRDD.compute()`.

`ExecutionSkipAcceptanceSpec` is the acceptance test, and it checks BOTH things a correct-results-
only test could pass on even if the substitution silently fell through to normal execution:
correct final rows AND fewer Spark tasks than an unresumed baseline. Observed for the fixture
query (`range(4 partitions).repartition(3).collect()`): baseline 7 tasks (4 upstream shuffle-map +
3 result), resumed 3 (only the result tasks, each reading its own partition) — the 4 upstream map
tasks are genuinely ELIMINATED. Uses `InMemoryExchangeStore` (made `Serializable` for exactly this
reason — its state is a plain, content-preserving `ConcurrentHashMap`, unlike a real backend's
store, which is never shipped live; see `ExecutionSkipRule`'s `storeFactory` doc comment), so this
module stays backend-agnostic; `spark-resume-integration`'s `skip` scenario proves the identical
mechanism survives a real cross-process boundary against `spark-resume-fs`.

**Capture statistics are honest placeholders when no real store is wired, real when one is.**
`SparkResumeListener` (whole-query) still has no per-stage hook and still writes disclosed
placeholders. `StageCaptureListener`'s ORIGINAL `None`-default `exchangeStore` behavior is
UNCHANGED (`NoHandleKind` sentinel, `rowCount=0`) — passing `Some(store)` is what turns on the new,
real capture path (real handle, real `rowCount` counted from actual encoded rows).

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

A fifth finding, the same bug shape hitting a NON-leaf node this time, found not by inspection but
by `spark-resume-integration`'s real cross-process testing: the generic (non-leaf) node branch's
`exprString` (`.expressions`-only, same accessor as above) silently dropped `RoundRobinPartitioning`
entirely — what a plain `df.repartition(n)`, no columns, produces. `RoundRobinPartitioning` does
not extend Catalyst's `Expression` (unlike `HashPartitioning`/`RangePartitioning`, which do, and so
were already visible by accident), so `df.repartition(3)` and `df.repartition(7)` over the
identical source hashed IDENTICALLY — a real A-1 false-positive-resumption hazard in Tier 1 itself,
not a connector-specific gap. Fixed by also hashing `node.outputPartitioning.toString`
(exprId-stripped) for every generic node — safe for any `SparkPlan`, not just `Exchange`, since
`outputPartitioning` is defined unconditionally on the base trait. See
`WholePlanFingerprint.partitioningString`'s doc comment and `WholePlanFingerprintSpec`'s two new
tests. No regression across the rest of the suite (28/28 after the fix, up from 26/26 before).

A sixth finding, positive this time, not a bug: this module's own long-standing claim ("no public
Spark 3.5 extension point exists for execution-skipping") was checked again directly against 3.5.1
source and found wrong. See "What this does NOT prove" above and `AqeExecutionSkipSpikeSpec` for
the two gates proven.
