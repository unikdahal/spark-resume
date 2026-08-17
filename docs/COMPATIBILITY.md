# Compatibility matrix

**Maintained by hand, not by CI** (`docs/DESIGN.md` §14 Phase 4 / §15: there is no CI in this
repository yet — see `CONTRIBUTING.md`). Every row below reflects what has actually been built and
tested, verified by re-running the commands shown, not aspirational support. If a cell doesn't
have a note, it means "not attempted," not "known to work."

## Spark line

| Spark version | Status | Notes |
|---|---|---|
| 3.5.9 | **Tested** | The only Spark line this project targets today. `spark-resume-spark-3.5`'s whole suite (30 tests as of this doc) runs against it in `mvn install`. |
| 3.0 – 3.4 | Not tested | `injectQueryStagePrepRule`/`injectQueryStageOptimizerRule` (docs/DESIGN.md §8's correction) are documented as present since Spark 3.0, but nothing in this repo has been run against any version other than 3.5.9. `WholePlanFingerprint`'s AQE-unwrap logic (`AdaptiveSparkPlanExec.initialPlan`, `QueryStageExec`) is specific to internals that may differ across minor lines — do not assume compatibility without testing. |
| 4.x | Not tested | See `spark-resume-spark-3.5/pom.xml`'s `provided`-scope Spark dependency — this module compiles against 3.5's API surface specifically; a 4.x-targeting module would need its own `spark-resume-spark-4.x` module (see `docs/DESIGN.md` §9: "One module per supported Spark line going forward, never a single module straddling incompatible internal APIs across lines"). |

## `ExchangeStore` implementations

| Backend | Module | `handleKind`/`isFresh`/`checkIdentityIsolation` | `reattach` | Tested against |
|---|---|---|---|---|
| In-memory | `spark-resume-api` (`InMemoryExchangeStore`) | Real | Real | The reference implementation the conformance testkit was originally written against. Dev/test only — no cross-process durability. |
| Apache Celeborn 0.7.0 | `spark-resume-celeborn` | Real, against a real cluster | **Not implemented** — documented `UnsupportedOperationException`, a checked Tier 3 backend capability gap (vanilla Celeborn's public client API has no cross-application shuffle read; confirmed via bytecode inspection, not assumed) | `CelebornExchangeStoreSpec` (`ExchangeStoreContract`, `reattachSupported = false`) against a real single-master/single-worker cluster stood up from the official binary release (`run-celeborn-tests.sh`). |
| Local filesystem | `spark-resume-fs` | Real | **Real** — the only backend in this repo where `reattach` actually succeeds; reads real files back off disk. Built specifically to prove the conformance testkit itself, not as a production backend. | `FsExchangeStoreSpec` (`ExchangeStoreContract`, `reattachSupported = true`, the default), `FsHandleCodecSpec`, `FsExchangeStoreDirectSpec` — all against a real local temp directory, zero external infrastructure. |

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
