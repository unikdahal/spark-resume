# Contributing to spark-resume

This is a young, solo-maintained project (see `docs/DESIGN.md` §16, named risk 5) with a strict
documentation discipline: every claim this project makes about what it proves is backed by a real
test against real infrastructure, and every known gap is disclosed loudly, not glossed over. PRs
are held to the same bar.

## Conformance tests are a hard gate for SPI implementations

Any new `AnchorStore` or `ExchangeStore` implementation must extend and pass
`org.apache.spark.resume.api.testkit.AnchorStoreContract` / `ExchangeStoreContract`
(`spark-resume-api`'s test-jar) before it will be merged, in-tree or accepted as a documented
out-of-tree implementation in the README's ecosystem list. This is not a formality — the fencing
and identity-isolation tests in those suites exist because real bugs were found by exactly these
kinds of checks; see `docs/DESIGN.md` §4 and §12 for the specifics.

Two hooks on `ExchangeStoreContract` exist specifically so an honest implementation with a real
backend limitation can still conform without lying about it — don't reach for either as a
shortcut:

- `reattachSupported: Boolean` (default `true`) — override to `false` **only** if your backend's
  `reattach` is a genuine, checked, permanent capability gap, the way `spark-resume-celeborn`'s is
  (vanilla Apache Celeborn's public client API has no way to read a shuffle's committed locations
  under a different application identity, confirmed against real bytecode, not assumed). A PR
  that sets this to `false` without the same level of verification `spark-resume-celeborn/README.md`
  shows will be asked to either implement `reattach` for real or produce that evidence.
- `conflictingIdentityHandle: Option[ExchangeHandle]` — `None` is a deliberate claim that your
  backend has no identity-reuse hazard (see `ExchangeStore.checkIdentityIsolation`'s doc comment
  for what that hazard looks like). Return it because you checked, not because you didn't think
  about it.

If the testkit itself turns out to be genuinely wrong or incomplete for your backend — not just
inconvenient — that's a real, valuable finding on its own (see `spark-resume-celeborn`'s history:
building it found `ExchangeStoreContract` unconditionally required `reattach` to succeed, which
the `reattachSupported` hook above exists to fix). Open an issue or PR against the testkit itself
with the same evidence standard as any other claim here: show the real test that fails and why
it's the testkit's fault.

Any new `SourceFingerprint` implementation must ship BOTH a positive conformance case (identical
source, identical fingerprint) and a negative one (a deliberately mutated source, a different
fingerprint). A fingerprint implementation without the negative case is not considered proven —
see `docs/DESIGN.md` §4, lesson 6, for why a positive-only test suite can pass even when the
fingerprint function is broken.

## Real infrastructure, not mocks

Wherever a claim is about behavior against a real backend, back it with a real test against a
real instance of that backend, not a mock. If your backend needs a running service, document
exactly how to start one — see `spark-resume-celeborn/run-celeborn-tests.sh` (an orchestration
script that stands up a real cluster from an official release) and `spark-resume-redis/README.md`
(a one-line container command) for the two existing patterns.

No reference to any private, patched, or non-upstream fork of a backend this project integrates
with. Every dependency must be a real, published, vanilla release — see
`spark-resume-celeborn/pom.xml`'s own comment on why this matters. A PR that needs a patched
backend to work is describing a Tier 3 gap (`docs/DESIGN.md` §8), not a working implementation.

## Disclosure, not silence

If your implementation has a known gap, say so in its module README the same way
`spark-resume-celeborn/README.md` and `spark-resume-iceberg/README.md` do — what's real, what
isn't, and exactly what was checked (not assumed) to reach that conclusion. If you found a real
bug while building your PR (this project's own history is full of these — see any module's
README's "real bugs found" section), say what you found, how you found it, and how you fixed or
disclosed it. Don't quietly fix it without mentioning it existed, and don't claim more than what
was actually tested.

## Scope discipline

Each module has a stated dependency boundary (see the header comment in each `pom.xml`). If
making a change requires adding a dependency a module's own doc comment says it shouldn't have,
that's a signal the change belongs in a different module, not a reason to loosen the boundary.

## Style

Short, concise doc comments that explain *why*, not just *what* — match the density already in
the codebase. Every non-obvious design decision should be traceable to either a stated invariant
(`docs/DESIGN.md` §7) or an empirical lesson (`docs/DESIGN.md` §4), not asserted without one.

## Local build

```
mvn install
```

JDK 17, Scala 2.12.18. Tests run via `scalatest-maven-plugin` (`mvn test`), not Surefire/JUnit —
see the root `pom.xml`'s comment. `spark-resume-redis` needs a real Redis reachable (see its
README); `spark-resume-celeborn` needs a real Celeborn cluster (see `run-celeborn-tests.sh`).
Everything else (`spark-resume-api`, `spark-resume-core`, `spark-resume-spark-3.5`,
`spark-resume-iceberg`, `spark-resume-fs`) needs no external infrastructure at all.

`spark-resume-integration` is not part of a bare `mvn install` (see its own README for why) — run
it explicitly via `spark-resume-integration/run-integration-test.sh` once you have a real Redis
and a real Celeborn cluster up.

## Before opening a PR

- `mvn install` passes, including tests, for every module you touched.
- New public API surface in `spark-resume-api` gets a doc comment explaining its contract,
  including failure modes — that module is what every third-party implementation compiles
  against, so its documentation is part of the interface.
- Paste the real command and its real output for any new "this works" claim — see any module's
  README for the standard this repo already holds itself to.

## No CI yet

There is no CI pipeline in this repository yet — every claim in this document and every README is
currently verified by hand, reproduced multiple times, not enforced automatically. That's a real,
disclosed gap (see `docs/DESIGN.md` §14 Phase 4), not an oversight. Until CI exists, a PR's "tests
pass" claim is exactly as trustworthy as the evidence the PR itself shows for it.

## Design questions

Read `docs/DESIGN.md` first, especially §2 (non-goals), §7 (the invariants every change must keep
proving, not just not-visibly-break), and §8 (the three-tier Spark integration strategy, including
the Phase 4 correction about `injectQueryStagePrepRule`). A change that would violate one of §7's
invariants (A-1 through A-6) needs to say so explicitly and explain why, not slip through silently.
