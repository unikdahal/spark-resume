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

**Phase 0 done, Phase 1 done, Phase 2 done with one known, disclosed correctness gap, Phase 3
underway (one real backend done, honestly partial).** `spark-resume-api` (the SPI) +
`spark-resume-core` (the admission engine, the identity-isolation-safe reattach path) +
`spark-resume-spark-3.5` (Tier 1 Spark 3.5 integration: real whole-plan AND per-stage
fingerprinting, a capture/check decision-layer proof against two independent `SparkSession`s at
both granularities) + `spark-resume-iceberg` (an Iceberg `SourceFingerprint` keyed on the resolved
snapshot id) + `spark-resume-redis` (the first real, cross-process `AnchorStore`, atomic fencing
proven against a real Redis server) + `spark-resume-celeborn` (the first real, cross-process
`ExchangeStore`, against a real vanilla Celeborn cluster) all build and are tested — 90/90 tests,
`mvn clean install` (needs a real Redis reachable for `spark-resume-redis` and a real Celeborn
cluster reachable for `spark-resume-celeborn` — see each module's README), reproduced clean across
multiple consecutive full runs. **Still no execution-skip mechanism anywhere in this repository**,
and two real, disclosed gaps ship rather than a fully clean bill of health: a confirmed A-1 race in
`spark-resume-iceberg`'s unpinned-read path (the default `FileSourceFingerprint` provider was
checked against the same race and confirmed NOT vulnerable — Iceberg-specific, not architectural),
and `spark-resume-celeborn`'s `reattach` is a documented `UnsupportedOperationException`, not a
working implementation — vanilla Apache Celeborn's public client API was checked and confirmed to
have no way to read a shuffle registered under a different application's identity, which is
`docs/DESIGN.md` §8's Tier 3 exactly as specified, a backend capability gap, not a bug in this
project's code. This project reports gaps it can't yet fix, not just the ones it can. Read
`spark-resume-spark-3.5/README.md`'s and `spark-resume-celeborn/README.md`'s "What this does NOT
prove" sections before assuming more than that. Nothing here should be described as
production-ready — see `docs/DESIGN.md` §14 for the
roadmap and what each remaining phase needs to add before that claim would be earned.

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
                           paths, and the admission checks that tie them to spark-resume-core.
                           See its own README for what it proves, what it doesn't, and the real
                           bugs found building it.
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
docs/DESIGN.md             the full architecture design: concepts, invariants, the SPI in depth,
                           the three-tier Spark integration strategy, and the roadmap.
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
`SafeReattach` end to end.

## Implementing your own store or fingerprint provider

Depend on `spark-resume-api`. If you're implementing `AnchorStore` or `ExchangeStore`, also
depend on `spark-resume-api`'s test-jar (`<type>test-jar</type>`) and extend
`org.apache.spark.resume.api.testkit.AnchorStoreContract` / `ExchangeStoreContract` — every test
in that suite is expected to pass before an implementation is considered proven; see
`docs/DESIGN.md` §12.

## License

Apache License 2.0 — see `LICENSE`.
