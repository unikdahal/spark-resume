# Design: a generic shuffle/stage resumption library for Apache Spark

Status: **Phase 0, Phase 1, and Phase 2 done** (with a known, disclosed A-1 gap in the Iceberg
provider's unpinned-read path — see §14 Phase 2). `spark-resume-api` (the SPI), `spark-resume-core`
(the admission engine + the identity-isolation-safe reattach path), `spark-resume-spark-3.5` (Tier
1 Spark integration: whole-plan AND per-stage fingerprinting, capture, and admission-check, proven
across two independent `SparkSession`s), `spark-resume-iceberg` (an Iceberg `SourceFingerprint`),
and `spark-resume-redis` (a real, cross-process `AnchorStore`) are all built and tested — 72/72
tests, reproduced clean across multiple full runs (Redis running), see the repo root README and
each module's own README. Still no execution-skip mechanism (Phase 3+, Tier 2/3). This document
specifies the architecture for the project as a whole — provisionally named `spark-resume` (see
Appendix A) — that generalizes a mechanism proven across several proof-of-concept repositories
into a real, pluggable, production-grade library. It intentionally contains no reference to any
prior private design note, prototype name, or internal repository; everything it specifies is
re-derived here from first principles and from the empirical lessons summarized in "What the
prototypes taught us" below. Two SPI shape decisions recorded here as designed were refined
slightly during Phase 0's actual implementation — noted inline in §6 where they occur.

## 1. Purpose

When a Spark driver dies mid-query and a *new* driver process starts to re-run the same logical
query, every completed shuffle stage's output is normally recomputed from scratch — even though,
with a disaggregated shuffle service (Celeborn, Uniffle, IPC-based cache tiers, etc.), the actual
shuffle bytes for those completed stages are often still sitting on remote storage, undamaged,
addressable, and reusable. Recomputing them anyway wastes the cluster resources and wall-clock
time that already went into producing them once.

This project's purpose is narrow and specific: **let a second driver process, running what it can
prove is the same logical query against the same logical inputs, skip stages whose shuffle output
a first driver process already produced and durably registered**, by resuming from an external,
pluggable exchange-store backend instead of recomputing. It is a *stage-level* mechanism, not a
task-level or an in-flight-task mechanism — an individual task's lost partial work is out of scope
by design (see Non-Goals).

## 2. Non-goals

- **Not a general Spark HA/fault-tolerance framework.** Spark's own task-retry and stage-retry
  machinery is untouched and remains the first line of defense; this project only adds a second,
  optional layer that activates specifically on full-driver-restart.
- **Not a checkpoint/savepoint system for streaming.** Streaming checkpointing already solves a
  different problem (resuming a stream from an offset) with a different, adequate mechanism.
- **Not a replacement for a shuffle service.** This project is a consumer of a disaggregated
  shuffle service's own durability guarantees, never a reimplementation of them. A shuffle
  service that cannot itself survive its client driver's death (in-process shuffle, most classic
  ESS deployments) cannot be resumed from at all — this is a structural precondition, disclosed
  loudly rather than worked around.
- **Not a solution to in-flight task loss.** A task actively running at the moment of driver
  death is genuinely lost work; this project bounds how much work is lost (to "whatever was
  in-flight, not the whole query"), it does not eliminate the loss.
- **Not multi-driver / concurrent-writer query sharing** in its first release. The design leaves
  room for it (see the fencing invariants below), but v1 targets the sequential case: one driver
  dies, one new driver resumes.

## 3. Core concepts

Four concepts recur throughout the design; naming them precisely up front avoids ambiguity later.

- **Fingerprint** — a stable, deterministic identifier for a unit of work (initially: a single
  shuffle-producing stage's physical subtree) such that two fingerprints computed from two
  independent executions are equal if and only if those executions would, if both completed
  successfully, produce equivalent output. "Equivalent" here specifically includes reading the
  same version/snapshot of any external data source, not merely the same query text — this
  distinction is the central correctness property of the whole system (see §7, invariant A-1).
- **Anchor** — the durable record a completed stage leaves behind: its fingerprint, the exchange
  store's own handle for the stage's actual output bytes, and enough metadata (partition/mapper
  counts, per-partition size/row-count statistics) for a resuming process to both re-attach to the
  bytes and correctly inform any adaptive planning that reads execution statistics.
- **Exchange store** — the pluggable backend holding the actual shuffle bytes and answering
  "does this fingerprint's output still exist and is it still fresh" — deliberately a distinct
  interface from the **anchor store**, the pluggable backend holding the lightweight anchor
  *metadata* records. Splitting these two concerns was itself a lesson from the prototypes: a
  metadata store (small records, needs low-latency point lookups, benefits from something like
  Redis) has entirely different operational characteristics than a bytes store (Celeborn's own
  storage tier, already durable, already the source of truth for the actual data).
- **Admission** — the decision, made once per candidate stage at the moment a new driver would
  otherwise schedule that stage's tasks, of whether to resume from an anchor or fall through to
  normal execution. Admission is a *gate*, not a *hint*: refusing to resume must always be safe
  (falls through to identical behavior as if this project were not installed); resuming must never
  be attempted unless every admission check has passed.

## 4. What the prototypes taught us

This section is deliberately empirical rather than aspirational — every claim below was tested,
not assumed, against real infrastructure (a real disaggregated shuffle service, a real Spark AQE
planner, a real Iceberg REST catalog) before being written down as a design constraint.

1. **Fingerprint computation must never trust a connector-owned scan node's `hashCode`/`toString`,
   and must never assume a generic tree-walk covers every physical node type.** A DSv1 file scan
   and a DSv2 batch scan need entirely different, connector-aware fingerprint logic; a fallback
   path that silently degrades to "collides with everything" for an unrecognized scan type is
   *worse* than one that refuses to admit at all, because it produces false-positive resumption —
   silently reusing output computed against stale source data. This is the single most important
   correctness property in the whole design (formalized as invariant A-1 below).
2. **Adaptive query execution changes what "the same stage" means mid-query, not just
   query-to-query.** A stage's own physical subtree can be re-planned after the fact by AQE
   without the query text changing at all (join strategy selection, skew splitting,
   partition coalescing). A fingerprinting scheme built only for a static, pre-AQE plan silently
   breaks the moment AQE is enabled — proven, not assumed, by watching a broadcast-vs-shuffle-join
   AQE replan change a downstream stage's identity in a way a naive whole-plan hash conflated with
   "different query."
3. **A resumed stage's statistics have to be re-injected into the SAME channel the query
   optimizer actually reads, not just the channel most obviously related to shuffle.** Adaptive
   optimizations (empty-relation propagation, skew-join detection, small-partition coalescing) can
   each read a *different* runtime-statistics accumulator than the one a naive implementation
   populates. Missing one produces a working-looking resumption that silently corrupts a later
   planning decision — caught only by deliberately exercising every adaptive rule the resumed
   stage's downstream plan could trigger, not by code review.
4. **A freshness check has to distinguish "this data still exists" from "this data has been
   superseded by a same-slot rewrite."** A remote store reporting a file's current length is not
   enough on its own; a naive length-only check both false-rejects fresh-but-differently-observed
   data and false-accepts a file that was truncated-then-rewritten to the same nominal size. The
   check needs to be built from the store's own explicit versioning/generation primitive if one
   exists, not synthesized from surface signals like length or mtime alone.
5. **A store-identity or application-identity concern can silently defeat correctness through a
   completely different subsystem than the one under test.** A resuming process that reuses the
   SAME backend-application identity as the process that produced the anchor it is choosing NOT to
   resume from can collide with that already-materialized state in the backend and get a
   silently-empty result instead of an error. This is not a hypothetical: it was found, not
   predicted, while building a test for an unrelated property (fingerprint discrimination against
   a mutated data source). It generalizes to a real production hazard — any driver restart under a
   stable application identity that does NOT end up resuming needs the backend layer to either
   guarantee isolation between the two incarnations or fail loudly, never silently. See invariant
   A-6 below.
6. **Positive-match testing alone does not prove a fingerprinting scheme works.** A test suite
   built entirely from "capture, then resume, then check the results match" would pass even if the
   fingerprint function returned a constant — it never exercises the *rejection* path. Real
   confidence requires an explicit negative case: mutate the underlying source between the
   captured run and the resuming run, and assert the stale fingerprint is correctly refused, not
   just that a matching one is correctly accepted. Any pluggable fingerprint implementation this
   project ships or accepts from a plugin author needs both a positive and a negative conformance
   test before it can be trusted.
7. **A single-mapper, single-file test case under-exercises per-mapper statistics logic.** Several
   real bugs in this space only manifest when a stage's shuffle output is split across more than
   one mapper — a demo or conformance suite that always happens to produce one file per stage
   silently never exercises that code path at all. Conformance suites need to deliberately force
   multi-mapper output, not assume the test data's natural shape will do it.

## 5. Architecture overview

```
                    ┌─────────────────────────────────────────────┐
                    │              Spark driver process             │
                    │                                                │
                    │   ┌──────────────┐        ┌─────────────────┐ │
                    │   │ Query planner │──────▶│ StageInterceptor │ │
                    │   │ (AQE-aware)   │        │   (SPI, Tier 2) │ │
                    │   └──────────────┘        └────────┬────────┘ │
                    │                                     │          │
                    │                          ┌──────────▼────────┐ │
                    │                          │  AdmissionEngine   │ │
                    │                          │  (fingerprint +    │ │
                    │                          │   rule chain)      │ │
                    │                          └──────┬─────┬───────┘ │
                    └─────────────────────────────────┼─────┼─────────┘
                                                        │     │
                                    ┌───────────────────┘     └────────────────┐
                                    ▼                                          ▼
                         ┌─────────────────────┐                  ┌─────────────────────┐
                         │     AnchorStore       │                  │   ExchangeStore      │
                         │  (SPI — metadata)     │                  │  (SPI — bytes/handle)│
                         │  e.g. Redis, SQL,     │                  │  e.g. Celeborn,      │
                         │  local file (dev only)│                  │  Uniffle, custom      │
                         └─────────────────────┘                  └─────────────────────┘
```

The driver-side pieces are all pure Scala/Java, have no dependency on any specific shuffle
backend, and talk to exactly two SPI boundaries: an `AnchorStore` for metadata and an
`ExchangeStore` for the actual bytes/handle. A given deployment plugs in one implementation of
each (they need not come from the same module — a Celeborn-backed `ExchangeStore` and a
Redis-backed `AnchorStore` are entirely independent choices).

## 6. SPI

Four interfaces form the pluggable surface. All are designed to be implementable without any
Spark-internal or backend-internal knowledge beyond what each signature documents.

The two refinements flagged in the status note above, made while actually implementing this in
Phase 0: `Anchor` persists an OPAQUE `handleKind`/`handlePayload` (bytes), never a live typed
`ExchangeHandle`, because an `AnchorStore` implementation must be writable without depending on
every `ExchangeStore` implementation whose handles it might ever be asked to store — a live handle
can carry backend-internal state that doesn't belong in a generic persisted record.
`SourceFingerprint` is typed against a `FingerprintTarget` wrapper, not bare `Any` and not a Spark
type directly — giving it a real type name without pulling a Spark dependency into this module;
an engine-integration module defines the concrete `FingerprintTarget` subtype its plan nodes get
wrapped as. Both are reflected in the signatures below, which are the actual, built, tested code
(`spark-resume-api`), not a plan for it.

```scala
package org.apache.spark.resume.api

/** A backend-agnostic handle to one completed stage's shuffle output. Implementations own
  * whatever internal addressing scheme their backend uses; this project never inspects it. */
trait ExchangeHandle

trait ExchangeStore {
  def handleKind: String
  def serializeHandle(handle: ExchangeHandle): Array[Byte]
  def deserializeHandle(payload: Array[Byte]): ExchangeHandle

  /** True if the backend still considers `handle`'s data valid and unsuperseded. Must be built
    * from the backend's own explicit versioning/generation primitive where one exists (lesson 4,
    * §4 above) -- a length-only or mtime-only check is not an acceptable implementation of this
    * method for any backend that has something stronger available. */
  def isFresh(handle: ExchangeHandle): Boolean

  /** Backend-specific isolation/identity guard (lesson 5, §4): MUST be called, and MUST return
    * IsolationOk, before any call to `reattach` -- `spark-resume-core`'s `SafeReattach` is the
    * one enforced choke point that guarantees this, so no caller has to remember it. */
  def checkIdentityIsolation(handle: ExchangeHandle): IsolationResult

  /** Re-attach the current process to `handle`'s data as if this process had produced it,
    * returning per-partition size/row statistics so the caller can populate whatever
    * optimizer-visible statistics channel its Spark integration layer needs (lesson 3, §4). */
  def reattach(handle: ExchangeHandle): ReattachResult
}

case class ReattachResult(numMappers: Int, numPartitions: Int, bytesByPartition: Array[Long],
                           rowCount: Long, mapperAttempts: Array[Int])
sealed trait IsolationResult
case object IsolationOk extends IsolationResult
case class IsolationConflict(reason: String) extends IsolationResult
```

```scala
package org.apache.spark.resume.api

/** The durable record store's contract -- three operations, deliberately minimal. */
trait AnchorStore {
  /** A monotonic fence for `queryId`: every write for this query must be gated on the value
    * returned here, and a write for a stale generation must be refused by `putAnchor`, so a
    * zombie writer past its generation can never corrupt a later writer's anchors. */
  def acquireGeneration(queryId: String): Long

  /** Refused (returns false) if `generation` is not the CURRENT generation for this anchor's
    * queryId at write time. */
  def putAnchor(generation: Long, anchor: Anchor): Boolean

  def loadAnchors(queryId: String): Seq[Anchor]
}

/** `handlePayload`/`handleKind`, not a live `ExchangeHandle` -- see this section's opening note. */
case class Anchor(schemaVersion: String, queryId: String, generation: Long, fingerprint: String,
                   handleKind: String, handlePayload: Array[Byte], numMappers: Int,
                   numPartitions: Int, bytesByPartition: Array[Long], rowCount: Long,
                   createdAtMs: Long)
```

```scala
package org.apache.spark.resume.api

/** An opaque wrapper around one candidate unit of work's engine-native representation -- see
  * this section's opening note for why this exists instead of typing SourceFingerprint against
  * `Any` or a Spark type directly. */
trait FingerprintTarget { def node: AnyRef }

/** Computes a stable identifier for one candidate unit of work. Implementations are matched to a
  * specific data-source connector (see lesson 1, §4) -- the SPI is deliberately narrow so a
  * connector author can ship a `SourceFingerprint` alongside their connector without depending on
  * anything beyond the `FingerprintTarget` shape their connector introduces. */
trait SourceFingerprint {
  /** True if this implementation knows how to fingerprint `target`. The engine tries registered
    * implementations in priority order and falls through to a non-discriminating, clearly-marked
    * degraded fingerprint (never a crash, per invariant A-3) if none match. */
  def supports(target: FingerprintTarget): Boolean

  /** Must be a PURE function of `target`'s already-resolved, already-planned state -- no I/O
    * beyond what is needed to read already-cached connector metadata, and must be safe to call
    * from a hot planning-path context. */
  def fingerprint(target: FingerprintTarget): String
}
```

```scala
package org.apache.spark.resume.api

/** One admission check in the chain `spark-resume-core`'s `AdmissionEngine` runs before allowing
  * a resume. Rules compose: ALL registered rules must pass for admission to proceed, and the
  * chain short-circuits at the first non-Admit verdict -- a later rule is never consulted after
  * an earlier rejection. A rule that cannot decide (insufficient information, backend
  * unavailable) must return `Abstain`, which is treated as a REJECT for admission purposes
  * (fail-closed, per invariant A-2) but is distinguished in the emitted decision from an explicit
  * `Reject` so an operator can tell "we don't know" from "we know this must not resume." A rule
  * that throws is treated by the engine exactly as if it had returned `Abstain`. */
trait AdmissionRule {
  def evaluate(candidate: AdmissionCandidate): AdmissionVerdict
}

case class AdmissionCandidate(queryId: String, fingerprint: String, anchor: Option[Anchor],
                               stageInfo: StageInfo)
case class StageInfo(stageId: Int, numPartitions: Int)
sealed trait AdmissionVerdict
case object Admit extends AdmissionVerdict
case class Reject(reason: String) extends AdmissionVerdict
case class Abstain(reason: String) extends AdmissionVerdict
```

## 7. Correctness invariants

These are the properties the whole design exists to uphold. Each is stated as a testable claim,
not an aspiration, and each maps to a concrete lesson in §4.

- **A-1 (no false-positive resumption).** A resumed stage's output must be output that a
  from-scratch execution of the identical logical work, against the identical logical inputs
  (including external data source content/version), would itself have produced. A fingerprint
  collision between two logically different executions is a correctness bug, full stop — worse
  than an unnecessary recompute (a false negative), because it is silent. Any `SourceFingerprint`
  implementation that cannot prove this for its connector must degrade to a fingerprint that
  cannot collide across sources it cannot distinguish (e.g., include the fully-qualified source
  identity even when it can't read a finer-grained version signal), never omit discrimination
  silently.
- **A-2 (fail-closed).** Every failure mode in the admission path — a store being unreachable, a
  rule that cannot decide, a fingerprint computation throwing — must resolve to "do not resume,"
  never to "resume anyway." Falling through to normal execution must always be behaviorally
  identical to this project not being installed at all.
- **A-3 (fingerprinting never crashes the query).** A `SourceFingerprint` implementation
  encountering a plan shape or connector version it wasn't built for must degrade to a
  non-discriminating-but-safe fallback (satisfying A-1's discrimination requirement at the
  coarsest available granularity), never throw an exception that aborts planning for the whole
  query.
- **A-4 (statistics parity).** Every optimizer-visible runtime-statistics channel that a live
  execution of the resumed stage's node type would have populated must be populated identically
  by a resumption, not just the most obviously shuffle-related one (lesson 3, §4).
- **A-5 (freshness is backend-authoritative).** An `ExchangeStore.isFresh` check must be built
  from the backend's own strongest available consistency primitive, never synthesized from
  surface signals the backend didn't intend as a freshness API (lesson 4, §4).
- **A-6 (identity isolation).** `checkIdentityIsolation` must be called and must pass before any
  reattach, and an `ExchangeStore` implementation whose backend has an identity-reuse hazard must
  detect and refuse a conflicting reuse loudly rather than let it surface as silent data loss
  downstream (lesson 5, §4) — a residual, disclosed gap in the prototypes this project's design
  explicitly closes rather than inherits.

## 8. Spark integration: a three-tier strategy

Spark exposes some of what this project needs as genuinely stable public API, some of it as
something that could reasonably become a new, narrowly-scoped public extension point, and some of
it as internal state with no extension point at all today. Being explicit about which tier a given
piece of integration lives in is what keeps this project honest about its own compatibility
posture, and is what makes any future upstream conversation about a new extension point a
concrete, scoped proposal rather than a request to expose everything.

- **Tier 1 — public API only.** `SparkListener`, `QueryExecutionListener`, `SparkPlan`/
  `LogicalPlan` traversal, and the DSv2 catalog/scan interfaces a `SourceFingerprint`
  implementation needs to read are all stable, public, already-supported extension points. As much
  of this project's logic as possible lives here, because it is the only tier with a real
  cross-version compatibility story.
- **Tier 2 — a proposed, narrowly-scoped new extension point.** Observing a stage's own AQE
  materialization moment — the single synchronous point where a fully-planned stage's physical
  subtree is known and its output is about to be (or has just been) produced — is not exposed by
  any existing public listener today; `SparkListener`'s stage-completion events fire from the
  scheduler side, not the AQE planner side, and arrive without the plan-subtree context this
  project's fingerprinting needs. This project specifies the exact shape of the extension point it
  needs (a single callback interface, called once per materialized `QueryStageExec`, given only
  already-public types) as a concrete, reviewable proposal — not a demand, and not something this
  project depends on existing to be useful today (see Tier 3).
- **Tier 3 — a documented backend-patch requirement.** Reusing a disaggregated shuffle service's
  already-committed output for a *different* application/attempt than the one that produced it is
  not something most shuffle-service clients expose as public API today, because most shuffle
  services were not designed with this reuse pattern in mind. This project treats that as a
  property of the *shuffle service*, not of Spark, and documents exactly what capability a given
  `ExchangeStore` implementation's backend needs to provide (a way to re-register an existing,
  externally-identified shuffle's committed locations under a new application/attempt) rather than
  working around its absence. A backend without this capability simply cannot back an
  `ExchangeStore` for this project — that is disclosed as a backend requirement, not hidden behind
  a workaround.

Where Tier 2's extension point does not exist in a given Spark version, this project's `Tier 2`
adapter degrades to Tier 1 only: fingerprinting/anchoring happens at coarser-grained points already
reachable through public listeners (whole-query capture, not per-stage), which is strictly less
capable but never incorrect — this is a capability degradation, governed by the same fail-closed
posture as every other admission check (A-2).

## 9. Module layout

```
spark-resume-api/          # the SPI only: ExchangeStore, AnchorStore, SourceFingerprint,
                            # AdmissionRule, and their data types. Zero Spark dependency.
spark-resume-core/         # AdmissionEngine, the rule-chain runner, generation/fencing logic.
                            # Depends on spark-resume-api only.
spark-resume-spark-3.5/    # Tier 1 + Tier 2 (where available) Spark integration for 3.5.x.
                            # One module per supported Spark line going forward, never a single
                            # module straddling incompatible internal APIs across lines.
spark-resume-store-memory/ # in-process AnchorStore, dev/test only, explicitly documented as such.
spark-resume-store-redis/  # AnchorStore backed by Redis, real CAS fencing.
spark-resume-fingerprint-iceberg/  # SourceFingerprint for Iceberg's DSv2 scan node.
spark-resume-fingerprint-files/    # SourceFingerprint for the built-in file-source scan node.
spark-resume-it/           # cross-module integration tests -- the ONLY module allowed to depend
                            # on more than one backend implementation at once, and required to
                            # include a negative (mutated-source) case per lesson 6, §4 for every
                            # fingerprint implementation shipped in-tree.
spark-resume-bench/        # load/scale tests -- explicitly out of `-it`'s scope; this is where a
                            # "does this hold at production partition counts" claim would need to
                            # be earned before ever being written down as proven.
```

An `ExchangeStore` implementation for a specific shuffle backend (Celeborn, or any other) is
intentionally NOT listed above as an in-tree module in this initial cut — see Appendix B for why
the first concrete `ExchangeStore` implementation should probably be built and proven as an
out-of-tree consumer of `spark-resume-api` before deciding whether it belongs in-tree at all.

**Update, Phase 3:** built in-tree directly instead, as `spark-resume-celeborn` (not
`spark-resume-store-celeborn` — actual module names across this whole section drifted from this
early design pass during real implementation, same as noted for §6's SPI shapes; take this
section as original intent, not a current file listing). Appendix B's precondition — the SPI
proven against the dev-only backend first — was already satisfied by Phase 0's
`ExchangeStoreContract` + `InMemoryExchangeStore` by the time Phase 3 started, so the
out-of-tree-first sequencing this paragraph describes did not end up applying.

## 10. Configuration surface

All configuration is namespaced under `spark.resume.*`, off by default, and every knob has a
documented, safe default that preserves A-2 (fail-closed) if the operator sets nothing beyond
enabling the feature and naming their store implementations.

| Key | Meaning | Default |
|---|---|---|
| `spark.resume.enabled` | Master switch. | `false` |
| `spark.resume.anchorStore.impl` | Fully-qualified `AnchorStore` implementation class. | (required if enabled) |
| `spark.resume.exchangeStore.impl` | Fully-qualified `ExchangeStore` implementation class. | (required if enabled) |
| `spark.resume.fingerprint.providers` | Ordered list of `SourceFingerprint` implementation classes to try. | built-in file-source only |
| `spark.resume.admission.rules` | Ordered list of additional `AdmissionRule` implementation classes. | empty |
| `spark.resume.onStoreUnavailable` | `degrade` (fall through to normal execution) or `fail` (fail the query). | `degrade` |
| `spark.resume.anchor.ttlMs` | Anchor staleness ceiling, independent of any backend freshness check. | 24h |

## 11. Observability

Every admission decision — admit, reject (with the specific rule and reason), or abstain — is
emitted as a structured event through a `SparkListener`-visible channel, not just a log line, so an
operator can build a dashboard of "resume rate," "reject reasons by category," and "abstain rate"
(the last one specifically flags a store-availability or environment problem, distinct from a
correctness-driven reject) without parsing text logs. Metrics are exposed through Spark's existing
metrics system (a `Source` registered the standard way), not a bespoke reporting path.

## 12. Testing strategy

- **Unit tests** for every SPI implementation shipped in-tree, run against the SPI contract alone
  (no real Spark session), covering the failure-mode requirements in §7 explicitly — a
  conformance suite any third-party `ExchangeStore`/`AnchorStore`/`SourceFingerprint` author is
  expected to run their own implementation against before shipping it.
- **Fingerprint conformance tests are required to include both a positive case (identical source,
  identical fingerprint) and a negative case (mutated source, different fingerprint) for every
  `SourceFingerprint` implementation** — lesson 6, §4 made this non-negotiable rather than a nice-
  to-have; a fingerprint implementation without a negative test is not considered proven.
- **Multi-mapper coverage is required, not incidental** — lesson 7, §4: any integration test
  exercising per-partition or per-mapper statistics logic must deliberately force more than one
  mapper's worth of output, rather than trusting default test-data sizing to produce it.
- **Integration tests** (`spark-resume-it`) run a real local Spark cluster with AQE enabled against
  a real (not mocked) instance of each in-tree store implementation, covering: correct-result
  resumption, cold-baseline task-count comparison (never comparing a resumed run's task count
  against a structurally different plan — lesson learned the hard way in the prototypes), the
  actual mechanism-not-coincidence check (a resumed stage's fingerprint provably traces back to
  the anchor that produced it), and the go/no-go negative case per fingerprint provider.
- **Kill-mid-execution tests** simulate a real driver-process death at a defined point (not just a
  simulated one) and verify the new driver correctly resumes completed stages and correctly
  recomputes in-flight ones.
- **Scale/load tests** (`spark-resume-bench`) are kept explicitly separate from the correctness
  suite and never conflated with it in reporting — a passing `-it` run proves correctness at small
  scale, not readiness at production scale, and this project's own documentation says so plainly
  rather than letting a green test suite imply more than it tested.

## 13. Compatibility and CI policy

- One Spark-integration module per supported Spark minor line (starting with 3.5). No module
  straddles two incompatible internal-API surfaces via reflection tricks that could silently break
  on a patch release — where Tier 2/3 integration requires reflection against non-public state
  (reflectively reading a package-private field or method, the way the prototypes needed to for
  one connector's scan-node internals), that reflection is isolated behind an interface with a
  documented, tested fallback for when it fails (A-3's crash-safety requirement applied at the
  integration-code level, not just the fingerprint level).
- CI builds and runs the full test suite (unit + integration, not bench) against every supported
  Spark line on every PR. A Spark line is only added to the support matrix once its own
  integration module's full test suite passes, not provisionally.
- Semantic versioning on `spark-resume-api` specifically — the SPI is the one surface third-party
  implementations depend on, and it is versioned and evolved more conservatively than the rest of
  the project.

## 14. Roadmap

1. **Phase 0 — SPI + core admission engine — DONE, in this repository.** `spark-resume-api` +
   `spark-resume-core`, no Spark integration yet: the rule-chain, fencing (including a real-
   concurrency test, not just sequential calls), and fail-closed semantics are proven against the
   in-memory `ExchangeStore`/`AnchorStore` and the conformance testkit both ship in-tree with
   them — 25/25 tests passing. What Phase 0 does NOT claim: this is a tested SPI plus an
   in-memory store, nothing more. No Spark dependency exists anywhere in this repository yet, and
   nothing here should be called production-ready until Phase 1+ lands with its own coverage.
2. **Phase 1 — Tier 1 Spark 3.5 integration + file-source fingerprinting — DONE, in
   `spark-resume-spark-3.5`.** A real physical-plan fingerprinter (`FileSourceFingerprint` for
   the built-in file-source scan, a disclosed generic fallback for everything else),
   `SparkResumeListener` (capture, via `QueryExecutionListener`) and `AdmissionCheck` (the check),
   proven end to end across two independent `SparkSession`s: correct `Admitted`/`Rejected`
   decisions, including the go/no-go mutated-source case. What Phase 1 does NOT claim: no
   execution is ever skipped — there is still no public extension point to substitute a stage's
   output for real execution (that's Tier 2/3, Phase 2+); this proves the DECISION layer only.
   Three real bugs found building and testing it (`AdaptiveSparkPlanExec.executedPlan` mutating in
   place mid-adaptive-execution vs. its immutable `initialPlan`; `Expression.canonicalized` not
   stripping `exprId`, invisible to a single-session test suite; `QueryExecutionListener`'s
   asynchronous delivery racing an immediate unregister, reproducing only ~1 run in 3–4 across
   full-suite runs) — see `spark-resume-spark-3.5/README.md` for the full account of each.
3. **Phase 2 — Tier 2 extension point (or its Tier-1-only degradation), per-stage fingerprinting,
   Iceberg fingerprint provider, Redis anchor store.** This is the phase that reaches parity with
   what the prototypes already proved, built cleanly and generically instead of ad hoc.
   - **Per-stage fingerprinting — DONE, in `spark-resume-spark-3.5`
     (`StageFingerprint`/`StageCaptureListener`/`StageAdmissionCheck`).** Resolves the reconciliation
     concern this section previously flagged, but differently than anticipated: rather than
     translating `initialPlan` tree POSITIONS into runtime-adapted stage positions, identity is
     made content-addressed across both plan generations — the check side fingerprints every
     `Exchange` subtree found in `initialPlan`, the capture side fingerprints each materialized
     `ShuffleQueryStageExec`'s own exchange subtree from the FINAL plan, and
     `ColumnarToRowExec`/`InputAdapter`/`WholeStageCodegenExec`/`AQEShuffleReadExec` (execution-detail
     wrappers with no counterpart in `initialPlan`) are treated as transparent so the identical
     logical subtree hashes identically on both sides. Verified by actually running a shuffle query
     and asserting digest equality across two independently-planned `DataFrame`s, not assumed. A
     real bug found in the process: the first capture-side implementation called `.collect` directly
     on `queryExecution.executedPlan` and silently found zero stages, because
     `AdaptiveSparkPlanExec` is itself a `LeafExecNode` (`.children == Nil`) — the exact same trap
     `WholePlanFingerprint`'s own doc comment already warns about, hit again by not reusing the
     lesson; fixed by unwrapping to `.executedPlan` internally before collecting. Real runtime
     statistics (`numMappers`/`numPartitions`/`bytesByPartition` from `ShuffleExchangeLike` and
     `MapOutputStatistics`, all real public accessors) replace Phase 1's honest placeholders for
     stage anchors specifically — the whole-query anchor `SparkResumeListener` writes is
     unchanged and still placeholder. NOT reattachable: no `ExchangeStore` is wired to per-stage
     anchors in this phase (`Anchor.handleKind` is the disclosed sentinel
     `StageCaptureListener.NoHandleKind`) — this proves per-stage IDENTITY and STATISTICS only.
     Disclosed, not tested: AQE's skew-join-split optimization was not specifically exercised: the
     content-addressing approach should be unaffected (it reads the shuffle's own write-side stats,
     not the coalesced/split read side), but that is reasoning from the mechanism, not a dedicated
     test, and should get one before this is trusted for a skew-heavy workload.

     What content-addressing does NOT resolve, stated plainly rather than left implicit: AQE
     re-planning ABOVE an already-materialized stage (e.g. converting a sort-merge join to a
     broadcast join off runtime stats) produces an upper exchange subtree in the final plan with no
     counterpart in `initialPlan` at all — its capture-side digest can never be produced on the
     check side. Fails closed (that stage is simply never matched, per A-2), but it means per-stage
     adoption reaches only the bottom-most stages of any query AQE re-plans this way, not the whole
     tree — confirmed narrowly (a join+aggregation query correctly yields 2 check-side candidates
     but only 1 captured stage, since `StageFingerprint` is deliberately scoped to
     `ShuffleQueryStageExec` and excludes `BroadcastQueryStageExec` — the broadcast candidate
     legitimately has no match), not exhaustively (the specific SMJ→BHJ runtime-conversion case
     above was reasoned about, not forced and observed).

     A second real finding, from deliberately forcing two structurally IDENTICAL exchanges over
     the same source (`spark.sql.exchange.reuse=false`, so Spark's own `ReuseExchange` rule
     doesn't merge them first): they DO produce the same digest — a real, reproduced collision, not
     hypothetical. Reasoned to be benign (a collision under this project's content-addressed scheme
     implies genuinely interchangeable computations, precisely the condition Spark's own
     `ReuseExchange` exists to detect), and confirmed end to end (both check-side candidates
     resolve to `Admitted`, no crash). A secondary finding surfaced by chasing this: whether BOTH
     same-digest anchors survive a capture is `AnchorStore`-implementation-specific, not part of
     the interface's contract — `InMemoryAnchorStore` dedupes same-fingerprint writes within one
     generation (a pre-existing, documented Phase 0 design choice), so only one of the two
     anchors survives there, while a store that appends (`RedisAnchorStore`'s `RPUSH`) would keep
     both. Both behaviors are safe for this project's purposes (admission only ever needs ONE
     matching anchor), so this is noted as a real inter-implementation difference worth knowing
     about, not a bug to fix.
   - **Iceberg fingerprint provider — DONE, in the new `spark-resume-iceberg` module
     (`IcebergFingerprintProvider`).** Fingerprints a table's resolved Iceberg snapshot id, not a
     file listing — cheaper and exact, since Iceberg's own commit model already provides an
     immutable point-in-time identity. A real finding in reaching it: neither `Scan.description()`
     nor `Scan.toString()` expose the snapshot id (verified identical before/after a committing
     INSERT), and the field that does, `SparkBatchQueryScan.snapshotId()`, is package-private —
     unreachable without reflecting into Iceberg's internals, which this project's public-API-only
     posture rules out. The path that IS public: `BatchScanExec.table` cast to Iceberg's own
     public `SparkTable` class, whose `snapshotId()`/`branch()`/`table()` accessors give
     everything needed, resolved in branch → pinned-snapshot → current-snapshot priority order (a
     branch name is a moving pointer and must never be fingerprinted directly — that would be
     exactly the false-positive-resumption hazard A-1 forbids). A separate module from
     `spark-resume-spark-3.5`, deliberately not referenced by `DefaultProviders.all`, so a user
     without Iceberg on their classpath never risks a `NoClassDefFoundError`. **KNOWN GAP, NOT
     FIXED, found by a dedicated test rather than assumed safe:** the unpinned-read path (plain
     `SELECT`, no `VERSION AS OF`) is confirmed to leak a real A-1 hazard — the identical
     `BatchScanExec`/`SparkTable` object, fingerprinted once at plan time and again after the
     query executes with an unrelated commit landing in between, returns a DIFFERENT (newer)
     snapshot id the second time, even though the query itself read the OLDER data. The real
     capture path always fingerprints post-execution, so a commit racing the listener can write an
     anchor describing data newer than what was actually captured. No public Iceberg 1.6.1 API
     was found to fix this (the field that IS fixed at scan-build time is package-private,
     verified by exhausting the real candidates, not assumed) — mitigating it needs resolving the
     fingerprint BEFORE execution rather than after, a capture-architecture change beyond this
     provider's own scope. `VERSION AS OF`-pinned reads are unaffected. See
     `spark-resume-iceberg/README.md` and `IcebergFingerprintProvider`'s doc comment for the full
     account. **Checked whether the DEFAULT provider has the same hole, not left as an inference:**
     `FileSourceFingerprint` (`spark-resume-spark-3.5`) was confirmed NOT vulnerable to this same
     race, by running the identical experiment — the same `FileSourceScanExec` node, fingerprinted
     before and after an on-disk overwrite with no re-plan in between, returns the SAME fingerprint
     both times (`FileSourceFingerprintSpec`'s dedicated "A-1 SAFETY" test), because
     `InMemoryFileIndex`'s file listing is fixed at plan time, not live-refreshing the way
     Iceberg's `Table` handle is — corroborated by the fact that actually executing against the
     stale plan after such an overwrite fails outright (`FileNotFoundException`, since the exact
     part-files it was planned against are gone) rather than silently reading the new data. This
     gap is confirmed Iceberg-connector-specific, not a property of this project's capture
     architecture in general.
   - **Redis anchor store — DONE, in the new `spark-resume-redis` module
     (`RedisAnchorStore`/`AnchorCodec`).** The first real, cross-process `AnchorStore`
     implementation — durable and visible to a genuinely different driver process, which is the
     whole point this project exists for. `acquireGeneration` is Redis's own atomic `INCR`;
     `putAnchor`'s compare-current-generation-then-write is a Lua script evaluated server-side via
     `EVAL`, so the check and the write happen as one atomic step rather than a client-side
     read-then-write race a concurrent writer could interleave with. Proven against a REAL Redis
     server (not a mock), including `AnchorStoreContract`'s 16-thread concurrent-writer test — the
     one scenario a non-atomic implementation only fails under genuine pressure. `Anchor`s are
     encoded via an explicit, versioned, length-prefixed wire format (`AnchorCodec`), not Java
     serialization, with a dedicated round-trip test that compares every field by CONTENT (`Anchor`
     is a case class with `Array` fields, whose generated `equals` is reference equality on them —
     a naive `shouldBe` comparison would silently test the wrong thing). Disclosed: `mvn clean
     install` at the repo root now requires a real Redis reachable at `REDIS_HOST`/`REDIS_PORT`
     and fails loudly (not a silent skip) without one — see `spark-resume-redis/README.md` for the
     one-line podman command.
   - **Phase 2 is now complete** in the sense of "built and tested with real infrastructure, real
     bugs found and either fixed or disclosed": per-stage fingerprinting, an Iceberg fingerprint
     provider, and a real cross-process anchor store. 72/72 tests across the repo, `mvn clean
     install` (with Redis running), reproduced clean. It is NOT complete in the sense of "safe to
     depend on for the unpinned-Iceberg-read case" — see the KNOWN GAP just above, which stayed
     open because no public API fix exists at this phase, not because it went unnoticed.
4. **Phase 3 — first real `ExchangeStore` implementation for a disaggregated shuffle backend —
   underway; the metadata half is done, `reattach` is a confirmed, documented gap, not yet
   closed.** Built in-tree directly (Appendix B's "prove the SPI against the dev-only backend
   first" precondition was already satisfied by `ExchangeStoreContract` + `InMemoryExchangeStore`
   from Phase 0, so the out-of-tree-first sequencing that section describes did not apply here).
   `spark-resume-celeborn`'s `CelebornExchangeStore`, against vanilla Apache Celeborn `0.7.0` (a
   real published release, not a private fork): `handleKind`, `serializeHandle`/
   `deserializeHandle`, `isFresh` (a live query against the master's admin REST API), and
   `checkIdentityIsolation` are real and tested against a real Celeborn master and worker
   (`ExchangeStoreContract`'s 8 tests, plus a `SafeReattach.attempt` end-to-end proof against the
   real cluster). `checkIdentityIsolation` is a genuine, tested closing of a hazard prior work on
   this idea only ever disclosed, never enforced in code: it refuses `IsolationConflict` when a
   resuming driver's own `appUniqueId` matches the anchor's producing `appUniqueId`.

   `reattach` throws a documented `UnsupportedOperationException` — Tier 3 exactly as this
   section's own header describes it, confirmed by checking vanilla Celeborn `0.7.0`'s actual
   client API (`ShuffleClient`/`LifecycleManager`) rather than assumed: `readPartition`/
   `registerMapPartitionTask` are scoped to the registering `LifecycleManager`; there is no public
   method to read a shuffle's committed locations under a different application's identity.
   `spark-resume-core`'s `SafeReattach` gained a fourth `ReattachOutcome`,
   `RefusedUnsupported`, specifically because of this: `store.reattach` throwing
   `UnsupportedOperationException` would otherwise propagate a raw exception out of the ONE
   enforced choke point every driver-side integration calls `reattach` through, rather than the
   structured refusal every OTHER gap in that function already produces. A real prior, private,
   unpublished prototype needed to add exactly this missing capability (non-upstream
   `LifecycleManager` methods) to make cross-application reattachment work at all — confirmation
   that vanilla Celeborn genuinely lacks it, not evidence this project failed to find an existing
   path.

   A real testkit finding surfaced building this: `ExchangeStoreContract`, as originally written,
   unconditionally required `reattach` to succeed in two of its eight tests — unsatisfiable by an
   honest Tier 3 implementation. Fixed by adding a `reattachSupported: Boolean = true` hook
   (default preserves every other implementation's existing behavior), mirroring the contract's
   existing `conflictingIdentityHandle: Option[...]` pattern for "not every backend has this
   hazard" — a deliberate documented claim about the backend when overridden, not a shortcut.

   Standing up a real Celeborn cluster for this needed the OFFICIAL binary distribution (Maven
   Central publishes only the Spark-bundled client and the admin REST client standalone, not the
   master/worker service jars) — `spark-resume-celeborn/run-celeborn-tests.sh` downloads it once,
   stands up a local single-master/single-worker cluster, runs the module's tests, and tears down
   on exit; reproduced clean across 2 consecutive runs from a cold standup. See
   `spark-resume-celeborn/README.md` for the full account.
5. **Phase 4 — scale/load validation, multi-fingerprint-provider conformance suite opened to
   external contributors, public 1.0.**

## 15. Presenting this project publicly

- A clear, example-driven README: what problem this solves, a runnable quickstart against the
  in-memory store (so a reader can try it with zero external infrastructure), and a pointer to the
  SPI docs for anyone wanting to plug in their own store or fingerprint provider.
- `CONTRIBUTING.md` describing the conformance-test requirement (§12) as a hard gate for any new
  SPI implementation PR, not a suggestion.
- A public compatibility matrix (Spark line × store implementation × fingerprint provider), kept
  current by CI rather than by hand.
- Every disclosed gap in this document (§2's non-goals, §7's invariants stated as claims this
  project must keep proving, not facts already established) stays visible in the public docs, not
  softened for presentation — the prototypes' own documentation discipline (state exactly what's
  proven, reproduced, and still open, every time) is a project value worth carrying forward, not
  an artifact of this being a private effort.
- Framed throughout as a general-purpose Spark extension, built against public Spark APIs and one
  well-scoped proposed extension point (§8) — reviewable, testable, and useful standalone,
  independent of any specific downstream Spark version or fork.

## 16. Named risks

1. **Tier 2's extension point may never land upstream**, or may land in a materially different
   shape than proposed here. Mitigated by the Tier 1-only degradation path (§8) always being a
   complete, if coarser, fallback — this project is useful without Tier 2, not blocked on it.
2. **A third-party `SourceFingerprint` implementation that skips the negative-case conformance
   test could ship a false-positive-resumption bug** (A-1) undetected until it causes silent data
   corruption in someone's production query. Mitigated by making the conformance suite a CI gate
   for in-tree providers and loudly documented as required (not optional) for out-of-tree ones.
3. **A/backend without Tier 3's required capability cannot be supported at all**, which may exclude
   otherwise-popular shuffle services. Disclosed as a hard requirement rather than worked around
   with a weaker, unsafe substitute.
4. **Operational complexity of running two additional stateful services** (an anchor store, and
   whatever the exchange store's backend needs) may be a real adoption barrier for smaller
   deployments. The in-memory/dev-only anchor store and clear "resume is entirely optional and
   fails closed" framing exist specifically to keep the barrier to *trying* this low, even though
   a real production deployment needs the real stores.
5. **Solo-maintainer bus factor**, same as any new open-source project. Mitigated over time by the
   conformance-suite structure itself lowering the bar for external SPI-implementation
   contributions without requiring deep core-engine knowledge.

## Appendix A: naming

Recommended: `spark-resume`. Short, accurately scoped (this is specifically about resuming Spark
work, not a general checkpoint framework), and does not overclaim generality it doesn't have yet.
Alternatives considered and rejected: anything implying "recovery" or "fault-tolerance" broadly
(overclaims — this is not a general FT framework, see Non-Goals), anything naming a specific
backend (Celeborn, Redis, Iceberg) in the project name itself (undercuts the explicit design goal
of being backend-agnostic via the SPI).

## Appendix B: what to build first

Given the roadmap in §14, the single highest-leverage first artifact is **Phase 0 + Phase 1**: the
SPI, the admission engine, and a working (if coarse) resumption path against the in-memory store
and the built-in file source. This is buildable and testable with zero external infrastructure,
proves out the rule-chain and fail-closed semantics in isolation from any backend-specific
complexity, and gives early external contributors something they can run and extend immediately.
Building a real `ExchangeStore` for a specific disaggregated shuffle backend is deliberately
sequenced *after* this — not because it's less important, but because the SPI boundary needs to be
proven stable against at least the dev-only backend before a real backend implementation locks in
assumptions about it that turn out to be wrong.
