# Compatibility matrix

**Maintained by hand, not by CI** (`docs/DESIGN.md` §14 Phase 4 / §15: there is no CI in this
repository yet — see `CONTRIBUTING.md`). Every row below reflects what has actually been built and
tested, verified by re-running the commands shown, not aspirational support. If a cell doesn't
have a note, it means "not attempted," not "known to work."

## Spark line

| Spark version | Status | Notes |
|---|---|---|
| 3.5.9 | **Tested** | The only Spark line this project targets today. `spark-resume-spark-3.5`'s whole suite (35 tests as of this doc, including a real execution-skip acceptance test and `ExecutionSkipRuleSpec`'s four multi-exchange substitution tests) runs against it in `mvn install`. |
| 3.0 – 3.4 | Not tested | `injectQueryStagePrepRule`/`injectQueryStageOptimizerRule` (docs/DESIGN.md §8's correction) are documented as present since Spark 3.0, but nothing in this repo has been run against any version other than 3.5.9. `WholePlanFingerprint`'s AQE-unwrap logic (`AdaptiveSparkPlanExec.initialPlan`, `QueryStageExec`) is specific to internals that may differ across minor lines — do not assume compatibility without testing. |
| 4.x | Not tested | See `spark-resume-spark-3.5/pom.xml`'s `provided`-scope Spark dependency — this module compiles against 3.5's API surface specifically; a 4.x-targeting module would need its own `spark-resume-spark-4.x` module (see `docs/DESIGN.md` §9: "One module per supported Spark line going forward, never a single module straddling incompatible internal APIs across lines"). |

## `ExchangeStore` implementations

| Backend | Module | `handleKind`/`isFresh`/`checkIdentityIsolation` | `reattach` / `store` / `readPartition` | Real execution-skip capable? | Tested against |
|---|---|---|---|---|---|
| In-memory | `spark-resume-api` (`InMemoryExchangeStore`) | Real | Real | Yes, same-process only — `Serializable` (Phase 4) specifically to drive `spark-resume-spark-3.5`'s `ExecutionSkipAcceptanceSpec` | The reference implementation the conformance testkit was originally written against. Dev/test only — no cross-process durability. |
| Apache Celeborn 0.7.0 | `spark-resume-celeborn` | Real, against a real cluster | **Not implemented, all three** — documented `UnsupportedOperationException`, a checked Tier 3 backend capability gap (vanilla Celeborn's public client API has no cross-application shuffle read/write; confirmed via bytecode inspection, not assumed) | **No** — same Tier 3 gap | `CelebornExchangeStoreSpec` (`ExchangeStoreContract`, `reattachSupported = false`) against a real single-master/single-worker cluster stood up from the official binary release (`run-celeborn-tests.sh`). |
| Local filesystem | `spark-resume-fs` | Real | **Real, all three** — reads/writes real, per-partition-addressable files off disk | **Yes, cross-process** — this repo's real proof target, since Celeborn's gap rules it out | `FsExchangeStoreSpec` (`ExchangeStoreContract`, `reattachSupported = true`, the default), `FsHandleCodecSpec`, `FsExchangeStoreDirectSpec` — all against a real local temp directory, zero external infrastructure. Also: `spark-resume-integration`'s `skip` scenario, real cross-process execution-skip. |

## `AnchorStore` implementations

| Backend | Module | Tested against |
|---|---|---|
| In-memory | `spark-resume-api` (`InMemoryAnchorStore`) | The reference implementation. Dev/test only. |
| Redis | `spark-resume-redis` | `RedisAnchorStoreSpec` (`AnchorStoreContract`, including a 16-thread concurrent-fencing test) against a real Redis server (see that module's README for the one-line podman command). |

## `SourceFingerprint` providers

| Source type | Module | Known gaps |
|---|---|---|
| File-based (`FileSourceScanExec`) | `spark-resume-spark-3.5` (`FileSourceFingerprint`) | None found. Explicitly checked against, and confirmed NOT vulnerable to, the same live-refresh race Iceberg has (see next row) — the default provider's file listing is fixed at plan time, not live-refreshing. |
| Apache Iceberg (DSv2) | `spark-resume-iceberg` | **A-1 race, unfixed, disclosed**: an unpinned read's post-execution fingerprint can drift to a snapshot NEWER than what was actually read, if an unrelated commit lands between planning and capture. Confirmed identically on Iceberg 1.6.1 and 1.10.2 — not version-specific. No public API fix exists at either version (the field that IS fixed at scan-build time, `SparkBatchQueryScan.snapshotId()`, is package-private). `VERSION AS OF`-pinned reads are unaffected. See `spark-resume-iceberg/README.md`. |

## `ExchangeStoreContract` / `AnchorStoreContract` conformance

Every real implementation above extends the matching testkit and passes 100% of it — see
`CONTRIBUTING.md` for why this is a hard gate, not a suggestion, for any new implementation.

## Real execution-skipping

| Component | Module | Status |
|---|---|---|
| `RowBytesCodec` | `spark-resume-spark-3.5` | Real — Spark's own `UnsafeRow` binary format, length-prefixed per row. |
| `ExecutionSkipRule` / `SkippedShuffleRDD` / `SkippedExchangeExec` | `spark-resume-spark-3.5` | Real — registered via `SparkSessionExtensions.injectQueryStagePrepRule` (public since Spark 3.0). Substitutes a real byte-reading leaf for an admitted, reattachable `Exchange`; falls through to normal execution on any precondition failure. |
| `StageCaptureListener`'s `exchangeStore` parameter | `spark-resume-spark-3.5` | Real when `Some(store)` — captures actual materialized row bytes and calls `store.store(...)`. `None` (default) preserves Phase 2's original stats-only, sentinel-handle behavior unchanged. |
| Acceptance proof, same process | `spark-resume-spark-3.5` (`ExecutionSkipAcceptanceSpec`) | Real, against `InMemoryExchangeStore` — correct final rows AND strictly fewer Spark tasks than an unresumed baseline (observed 7 → 3 for the fixture query). |
| Acceptance proof, real cross-process | `spark-resume-integration` (`skip` scenario) | Real, against `spark-resume-fs` — identical acceptance criteria, proven across two separate OS processes with real files on disk as the durability layer. |
| Same capability against Celeborn | — | **Not possible** — `CelebornExchangeStore.store`/`readPartition` are the same Tier 3 `UnsupportedOperationException` as `reattach`. |
