# spark-resume

A pluggable library for Apache Spark that lets a new driver process skip re-computing shuffle
stages a previous, crashed driver already produced — if, and only if, it can prove those stages
would produce the same output again. Built against Apache Spark 3.5.

## The problem

When a Spark driver dies mid-query, Spark's own retry machinery starts a fresh query from
scratch. With a disaggregated shuffle service, though, the shuffle bytes a completed stage
already produced are often still sitting on remote storage — intact, addressable, reusable. This
project lets a new driver detect that, verify it's safe, and skip straight to reusing that output
instead of recomputing it.

It is a *stage-level* resumption mechanism, not a general fault-tolerance framework: Spark's own
task/stage retry logic is untouched, and this project only adds an optional layer that activates
on a full driver restart. See [`docs/DESIGN.md`](docs/DESIGN.md) for the full design, including
what this project explicitly does *not* attempt to solve.

## Status

**Phase 0–2 done, Phase 3 done (one real Tier 3 backend, honestly partial), Phase 4 underway —
including, as of the most recent work, a REAL execution-skip mechanism, not just the
decision-and-refusal plumbing every earlier phase built.** `spark-resume-api` (the SPI, now
including `ExchangeStore.store`/`readPartition`, the producer/consumer byte path) +
`spark-resume-core` (the admission engine, the identity-isolation-safe reattach path) +
`spark-resume-spark-3.5` (Tier 1 Spark 3.5 integration, PLUS `RowBytesCodec`/`ExecutionSkipRule`/
`SkippedShuffleRDD` — a real Spark stage's execution, genuinely skipped, real bytes read per
partition on the executor that consumes them, not funneled through the driver) +
`spark-resume-iceberg` + `spark-resume-redis` + `spark-resume-celeborn` (metadata-real, `reattach`/
`store`/`readPartition` all the same documented Tier 3 gap) + `spark-resume-fs` (a second,
independent `ExchangeStore` — filesystem-backed, real per-partition files, zero external
infrastructure, and this repo's real proof target for execution-skipping since Celeborn's gap
rules it out) all build and are tested — see `docs/COMPATIBILITY.md` for the current whole-repo
test count and what's proven where, `mvn clean install` (needs a real Redis reachable for
`spark-resume-redis` and a real Celeborn cluster reachable for `spark-resume-celeborn` — see each
module's README), reproduced clean across multiple consecutive full runs (verified 3x back-to-back
with zero flakes both before and after execution-skipping was added).

`spark-resume-integration` (a separate, not-bundled-in-`mvn install` proof — see its own README
for why) runs FIVE real scenarios across two genuine OS processes each: four are the Tier 3
refusal proof (`admitted` → `RefusedUnsupported`, `stale` → `RefusedStale`, `isolation-conflict` →
`RefusedIsolationConflict`, `miss` → every decision rejected), and the fifth, **`skip`, is the
real execution-skip proof across a real process boundary**: `ProcessA` captures real row bytes and
writes a real anchor via `spark-resume-fs`, exits; `ProcessB`, a separate JVM, resumes with
`ExecutionSkipRule` registered and gets the SAME final rows with STRICTLY FEWER Spark tasks than
an unresumed baseline — not a silent success, and not just "doesn't crash." Run explicitly via
`spark-resume-integration/run-integration-test.sh`.

Along the way, two real bugs were found by this project's own testing, not by inspection: a Tier 1
fingerprint gap (`repartition(3)`/`repartition(7)` hashed identically — `RoundRobinPartitioning`
isn't a Catalyst `Expression`, invisible to the walker — fixed, with regression tests; see
`spark-resume-spark-3.5/README.md`), and a stale architectural claim (this README and
`docs/DESIGN.md` §8 used to say Spark exposed no public hook early enough to intercept a stage's
execution — checked again against real Spark 3.5.1 source and found WRONG:
`SparkSessionExtensions.injectQueryStagePrepRule`, public since Spark 3.0, is exactly that hook,
and is now what `ExecutionSkipRule` is built on).

Two real, disclosed gaps still ship rather than a fully clean bill of health: a confirmed A-1 race
in `spark-resume-iceberg`'s unpinned-read path (the default `FileSourceFingerprint` provider was
checked against the same race and confirmed NOT vulnerable — Iceberg-specific, not architectural),
and `spark-resume-celeborn`'s `reattach`/`store`/`readPartition` are all the same documented
`UnsupportedOperationException` — vanilla Apache Celeborn's public client API was checked and
confirmed to have no way to read (or write, under a foreign identity) a shuffle's data, which is
`docs/DESIGN.md` §8's Tier 3 exactly as specified, a backend capability gap, not a bug in this
project's code. This project reports gaps it can't yet fix, not just the ones it can — read each
module's README's "What this does NOT prove" section before assuming more than that. Nothing here
should be described as production-ready — see `docs/DESIGN.md` §14 for the roadmap and what
remains (scale/load validation against real production infrastructure this project doesn't have
access to; the two upstream PRs, deferred; CI, out of scope for now) before that claim would be
earned.

## Repository layout

```
spark-resume-api/         the SPI: ExchangeStore, AnchorStore, SourceFingerprint, AdmissionRule,
                           and their plain data types, plus an in-memory reference implementation
                           of each store (dev/test only -- no cross-process durability) and the
                           conformance testkit (AnchorStoreContract / ExchangeStoreContract) any
                           real implementation is expected to pass. Zero Spark dependency.
spark-resume-core/        the admission engine (the rule-chain runner) and SafeReattach, the
                           single enforced choke point through which this project ever calls
                           ExchangeStore.reattach. Depends on spark-resume-api only.
spark-resume-spark-3.5/   Tier 1 Spark 3.5 integration: real physical-plan fingerprinting, both
                           whole-query and per-stage (file-source scans + a generic, disclosed
                           fallback for anything else), QueryExecutionListener-based capture
                           paths, and the admission checks that tie them to spark-resume-core --
                           PLUS RowBytesCodec/ExecutionSkipRule/SkippedShuffleRDD, a real
                           execution-skip mechanism (SparkSessionExtensions
                           .injectQueryStagePrepRule, correct results AND fewer Spark tasks,
                           proven). See its own README for what it proves, what it doesn't, and
                           the real bugs found building it.
spark-resume-iceberg/     an Iceberg SourceFingerprint keyed on the resolved snapshot id, not a
                           file listing. A separate module so a user without Iceberg on their
                           classpath never pulls it in. See its own README.
spark-resume-redis/       the first real, cross-process AnchorStore: Redis-backed, atomic
                           generation fencing (server-side INCR + a Lua compare-and-write script),
                           proven against a real Redis server. Its tests need one running -- see
                           its own README.
spark-resume-celeborn/    the first real, cross-process ExchangeStore: Apache Celeborn-backed.
                           The metadata half (handleKind/isFresh/checkIdentityIsolation) is real
                           against a real cluster; reattach is a documented
                           UnsupportedOperationException, a checked Tier 3 backend capability gap,
                           not a bug. Its tests need a real cluster -- run-celeborn-tests.sh stands
                           one up from the official release. See its own README.
spark-resume-fs/          a second, independent ExchangeStore: filesystem-backed, real
                           per-partition files, no external infrastructure. Proves
                           ExchangeStoreContract is satisfiable by someone who didn't write it,
                           and is this repo's real proof target for execution-skipping (Celeborn's
                           Tier 3 gap rules it out). See its own README.
spark-resume-integration/ not a library: a real TWO-PROCESS proof (ProcessA/ProcessB, two
                           separate mvn/JVM invocations) that the whole pipeline composes end to
                           end against real backends, across five scenarios -- four terminating in
                           the documented Tier 3 refusal (Celeborn), one (skip) a REAL execution
                           skip (fs): correct rows, fewer tasks, across a real process boundary.
                           See its own README.
docs/DESIGN.md             the full architecture design: concepts, invariants, the SPI in depth,
                           the three-tier Spark integration strategy, and the roadmap.
docs/COMPATIBILITY.md      Spark line x store implementation x fingerprint provider, maintained
                           by hand (no CI yet -- see CONTRIBUTING.md).
CONTRIBUTING.md            the conformance-testkit requirement as a hard gate for any new SPI
                           implementation PR, and what evidence a PR is expected to show.
```

## Building

```
mvn install
```

JDK 17, Scala 2.12.18 (matching Spark 3.5's default). Tests run via `scalatest-maven-plugin`
(`mvn test`), not the standard Surefire/JUnit path — see the root `pom.xml`'s comment on why
Surefire is deliberately disabled in this repo.

## Trying it with zero external infrastructure

`spark-resume-api`'s `InMemoryAnchorStore` / `InMemoryExchangeStore` (package
`org.apache.spark.resume.api.memory`) let you exercise the full admission engine — the rule
chain, the fencing semantics, the identity-isolation guard — without standing up any real
backend. They are explicitly NOT a production store: no persistence, no cross-process visibility,
which makes them useless for the one thing this project exists for (surviving a driver *process*
restart) — but they are a real, fully conformance-tested implementation of the SPI, and the
fastest way to see how the pieces fit together. See the test suites in
`spark-resume-core/src/test/scala` for worked examples of driving `AdmissionEngine` and
`SafeReattach` end to end. `InMemoryExchangeStore` is also `Serializable` (Phase 4) specifically so
it can drive `spark-resume-spark-3.5`'s `ExecutionSkipAcceptanceSpec` — a real, same-JVM proof of
actual execution-skipping with zero external infrastructure; see that module's README.

For a real, cross-process execution skip against real files (not just same-JVM), see
`spark-resume-fs` and `spark-resume-integration`'s `skip` scenario.

## Implementing your own store or fingerprint provider

Depend on `spark-resume-api`. If you're implementing `AnchorStore` or `ExchangeStore`, also
depend on `spark-resume-api`'s test-jar (`<type>test-jar</type>`) and extend
`org.apache.spark.resume.api.testkit.AnchorStoreContract` / `ExchangeStoreContract` — every test
in that suite is expected to pass before an implementation is considered proven; see
`docs/DESIGN.md` §12.

## License

Apache License 2.0 — see `LICENSE`.
