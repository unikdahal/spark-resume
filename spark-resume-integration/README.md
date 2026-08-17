# spark-resume-integration

The cross-process, cross-backend composition proof no other module in this repo provides: every
module up to this point is proven only in isolation against its own real backend. Nothing else
proves the SPI actually composes: a real Spark capture writing a real anchor to a real Redis, a
second, separate driver process reading it back, running it through the real `AdmissionEngine`,
and either reattaching for real (`skip` scenario, against `spark-resume-fs`) or being refused by
the documented Tier 3 gap (every other scenario, against `spark-resume-celeborn`).

Not a library. This module is never depended on by anything else and publishes no test-jar; it
exists solely to run `ProcessA`/`ProcessB` against real infrastructure.

## What it proves, and what it doesn't

Two SEPARATE JVM processes, each its own `mvn test -Dsuites=...` invocation (not two specs sharing
one JVM — see `ProcessASpec`/`ProcessBSpec`'s doc comments for why that split is load-bearing, not
incidental: a single JVM proves nothing about cross-process durability, which is the whole reason
this project exists), run through FIVE real scenarios (`INTEGRATION_SCENARIO`, see `ProcessB`'s
doc comment for the full table): `admitted` → `RefusedUnsupported`, `stale` → `RefusedStale`,
`isolation-conflict` → `RefusedIsolationConflict`, `miss` → every decision `RejectedBy` (no
`SafeReattach` call at all), and **`skip` → a REAL execution skip**: correct final rows AND
genuinely fewer Spark tasks than an unresumed baseline, real bytes written by `ProcessA` and read
back by `ProcessB` through `spark-resume-fs`. Every terminal state this pipeline can honestly reach
today, proven against real backends, not just the one happy path.

## The `skip` scenario — real execution-skipping, across a real process boundary

`ProcessA` uses the REAL `StageCaptureListener` (registered as an actual listener, not the manual
anchor-building the Celeborn-backed scenarios below use to fabricate a handle) with its
`exchangeStore` parameter set to a real `FsExchangeStore` — capturing the query's ACTUAL row bytes
and writing a real, reattachable anchor, then exiting. `ProcessB`, a separate JVM, builds a fresh
`SparkSession` with `ExecutionSkipRule` registered via `injectQueryStagePrepRule` BEFORE the
session exists, runs the IDENTICAL query, and asserts both: the resulting rows match an unresumed
baseline run within the same process, AND the resumed run submits strictly fewer Spark tasks —
observed at exactly the numbers `spark-resume-spark-3.5`'s own `ExecutionSkipAcceptanceSpec`
found in-process (7 baseline, 3 resumed), now reproduced across two real OS processes with real
files on disk as the durability layer, not `InMemoryExchangeStore`. See
`spark-resume-spark-3.5/README.md` for the full mechanism (`RowBytesCodec`/`ExecutionSkipRule`/
`SkippedShuffleRDD`) this scenario exercises end to end.

## The other four scenarios — the documented Tier 3 refusal, proven the same way

- **`ProcessA` (the producing side).** A real local `SparkSession` runs a real shuffle query
  (`Fixture.query`, a plain `repartition` — the simplest shape guaranteed to produce exactly one
  `Exchange`) to completion. Its shuffle stage is fingerprinted for real via
  `StageFingerprint.capturedStages` — the exact function `StageCaptureListener` itself calls. A
  real Celeborn shuffle is registered (the same `LifecycleManager`/`ShuffleClient` API
  `spark-resume-celeborn`'s own conformance fixtures use). One real `Anchor` is written to a real
  Redis, carrying that real `CelebornHandle` — **not** the `StageCaptureListener.NoHandleKind`
  sentinel Phase 2 ships. Then the process exits.
- **`ProcessB` (the resuming side).** A separate JVM, a fresh `SparkSession`, and a
  `resumingAppUniqueId` deliberately different from `ProcessA`'s `producingAppUniqueId` — exactly
  the identity split `CelebornExchangeStore.checkIdentityIsolation` exists to guard. Builds the
  IDENTICAL query (proving cross-*session* fingerprint stability composes across a real OS process
  boundary too, not just within one test JVM — see `Fixture`'s doc comment), reads `ProcessA`'s
  anchor back from Redis, runs `StageAdmissionCheck.check` (a real `AdmissionEngine` decision), and
  for the `Admitted` stage calls `SafeReattach.attempt` against the real Celeborn cluster.

The terminal outcome is `RefusedUnsupported`, and `ProcessB` asserts exactly that (a non-zero
exit/failed test otherwise). That is the honest end state this pipeline reaches today: real
cross-process fingerprint match, real cross-process anchor load, a real `Admitted` decision, and
`SafeReattach.attempt` reaching all the way to the real Celeborn cluster before being refused by
the one documented, checked Tier 3 gap (`CelebornExchangeStore.reattach` — see
`spark-resume-celeborn/README.md`). A test asserting `RefusedUnsupported` at the end of a full real
pipeline is stronger evidence than three modules each passing alone: it proves everything up to the
backend byte-read composes correctly across real processes, and that the one thing that doesn't
work is exactly, and only, the already-disclosed gap — not a silent success, and not a raw crash.

For `admitted`/`stale`/`isolation-conflict`/`miss`: `ExchangeStore` has no producer-side "issue me
a handle" method for Celeborn's use case (real Celeborn shuffle ids need to be wired to a specific
shuffle manager's internal id scheme, not just `store`/`readPartition`'s opaque bytes) — every
Celeborn-facing method on that trait is a resuming-driver operation (see `ExchangeStore.scala`).
`ProcessA` builds the real `CelebornHandle` itself for these scenarios, exactly the way an operator
manually wiring these two systems together today would have to. The terminal outcome for
`admitted` is `RefusedUnsupported`, and `ProcessB` asserts exactly that: real cross-process
fingerprint match, real cross-process anchor load, a real `Admitted` decision, and
`SafeReattach.attempt` reaching all the way to the real Celeborn cluster before being refused by
the one documented, checked Tier 3 gap (`CelebornExchangeStore.reattach` — see
`spark-resume-celeborn/README.md`). Stronger evidence than three modules each passing alone: it
proves everything up to the backend byte-read composes correctly across real processes, and that
the one thing that doesn't work is exactly, and only, the already-disclosed gap.

## Running it — needs a real Redis and a real Celeborn cluster already up

```
REDIS_HOST=localhost REDIS_PORT=16379 ./run-integration-test.sh
```

Does NOT stand up either backend itself (unlike `spark-resume-celeborn/run-celeborn-tests.sh`):
both are already-proven, already-documented infrastructure this module composes against, not a
third variant of the same clusters to maintain. Fails loudly, before running anything, if either
is unreachable. Runs all five scenarios in sequence, each its own fresh `INTEGRATION_QUERY_ID` and
its own ProcessA/ProcessB pair (`skip` additionally gets its own fresh `FS_STORE_BASE_DIR`, a real
temp directory, so repeated runs never collide). Reproduced clean across repeated consecutive
runs — 3x back-to-back after the `skip` scenario was added, zero flakes.

## A real bug this module's own testing found, in `spark-resume-spark-3.5`

Extending `ProcessB` to a `"miss"` scenario (a structurally different query — a different
`repartition(n)` count — expected to find no matching anchor) unexpectedly still got `Admitted`
against the real pipeline. Root cause, confirmed by direct probe: `WholePlanFingerprint`'s generic
node branch fingerprinted a node via its class name plus `.expressions` only.
`RoundRobinPartitioning` (what a plain `df.repartition(n)`, no columns, produces) does not extend
Catalyst's `Expression` — unlike `HashPartitioning`/`RangePartitioning`, which do, and so were
already visible by accident — so it was invisible to that walker entirely:
`df.repartition(3)` and `df.repartition(7)` over the identical source hashed IDENTICALLY. A real
A-1 false-positive-resumption hazard in Tier 1 itself, not a connector-specific gap, found by this
module's own real cross-process, cross-backend testing rather than by inspection. Fixed in
`WholePlanFingerprint` by also hashing `node.outputPartitioning.toString` (exprId-stripped, same as
every other fingerprint input here) for every generic node — safe for any `SparkPlan`, not just
`Exchange`, since `outputPartitioning` is defined unconditionally on the base trait. See
`WholePlanFingerprint.partitioningString`'s doc comment and
`WholePlanFingerprintSpec`'s two new committed tests for the full account. No regression across
the rest of `spark-resume-spark-3.5`'s suite (28/28 after the fix, up from 26/26 before).

## A real debugging note

The first implementation ran `ProcessA`/`ProcessB` as plain `java -cp ...` processes with a
hand-assembled classpath and the same JDK 17 `--add-opens` flags every other Spark module in this
project needs. That hit an unexplained `IllegalAccessError` (`sun.nio.ch.DirectBuffer` not
exported) from `Spark`'s own `StorageUtils`, inconsistently — the identical flags work for every
other real `SparkSession` in this repo when launched via `scalatest-maven-plugin`'s fork. Rather
than chase a JPMS discrepancy between a hand-rolled `java` invocation and Maven's own forking
further, `ProcessA`/`ProcessB` were wrapped as thin scalatest specs
(`ProcessASpec`/`ProcessBSpec`) run via `scalatest-maven-plugin`'s already-proven forking instead —
this module's actual process boundary is now "one `mvn test -Dsuites=...` invocation per process,"
not "one `java` invocation per process," and gets the correct classpath/module-opens for free from
infrastructure this repo already trusts.
