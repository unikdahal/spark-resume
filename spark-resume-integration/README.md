# spark-resume-integration

The cross-process, cross-backend composition proof no other module in this repo provides: every
module up to this point (`spark-resume-spark-3.5`, `spark-resume-redis`, `spark-resume-celeborn`)
is proven only in isolation against its own real backend. Nothing else proves the SPI actually
composes: a real Spark capture writing a real anchor to a real Redis, a second, separate driver
process reading it back, running it through the real `AdmissionEngine`, and calling
`SafeReattach.attempt` against a real Celeborn cluster.

Not a library. This module is never depended on by anything else and publishes no test-jar; it
exists solely to run `ProcessA`/`ProcessB` against real infrastructure.

## What it proves, and what it doesn't

Two SEPARATE JVM processes, each its own `mvn test -Dsuites=...` invocation (not two specs sharing
one JVM — see `ProcessASpec`/`ProcessBSpec`'s doc comments for why that split is load-bearing, not
incidental: a single JVM proves nothing about cross-process durability, which is the whole reason
this project exists):

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

**No execution is skipped anywhere in this repository, still.** This module does not resume a
query; it proves the decision-and-refusal path composes end to end.

## The seam this module had to bridge, and why it isn't in spark-resume-spark-3.5

`ExchangeStore` has no producer-side "issue me a handle" method — every method on that trait is a
resuming-driver operation (see `ExchangeStore.scala`). `StageCaptureListener` (Phase 2, committed,
already proven) is therefore correct to always write the `NoHandleKind` sentinel: it genuinely has
no way to know what Celeborn shuffle id a given Spark shuffle stage maps to. `ProcessA`
deliberately does NOT reuse `StageCaptureListener`; it builds the real `CelebornHandle` itself,
exactly the way an operator manually wiring these two systems together today would have to. Closing
this gap for real — either a producer-side method on the SPI, or wiring `spark-resume-spark-3.5` to
a specific shuffle manager's internal id scheme — is a real design decision for a future phase, not
this one. See `ProcessA`'s doc comment for the full account.

## Running it — needs a real Redis and a real Celeborn cluster already up

```
REDIS_HOST=localhost REDIS_PORT=16379 ./run-integration-test.sh
```

Does NOT stand up either backend itself (unlike `spark-resume-celeborn/run-celeborn-tests.sh`):
both are already-proven, already-documented infrastructure this module composes against, not a
third variant of the same clusters to maintain. Fails loudly, before running anything, if either
is unreachable. Reproduced clean across repeated consecutive runs.

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
