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

**Phase 0, in progress.** `spark-resume-api` (the SPI) and `spark-resume-core` (the admission
engine and the identity-isolation-safe reattach path) exist, build, and are tested — 25/25 tests
passing, including a real-concurrency fencing test and full coverage of the fail-closed admission
semantics. **There is no Spark integration yet.** Nothing in this repository should be described
as production-ready until a real engine integration (Phase 1+) exists and has its own test
coverage — see the roadmap in `docs/DESIGN.md` §14.

## Repository layout

```
spark-resume-api/    the SPI: ExchangeStore, AnchorStore, SourceFingerprint, AdmissionRule, and
                      their plain data types, plus an in-memory reference implementation of each
                      store (dev/test only -- no cross-process durability) and the conformance
                      testkit (AnchorStoreContract / ExchangeStoreContract) any real
                      implementation is expected to pass. Zero Spark dependency.
spark-resume-core/   the admission engine (the rule-chain runner) and SafeReattach, the single
                      enforced choke point through which this project ever calls
                      ExchangeStore.reattach. Depends on spark-resume-api only.
docs/DESIGN.md        the full architecture design: concepts, invariants, the SPI in depth, the
                      three-tier Spark integration strategy, and the roadmap.
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
