# Contributing

Thanks for considering a contribution. A few things that keep this project's correctness claims
trustworthy, not just fast to merge:

## Conformance tests are a hard gate for SPI implementations

Any new `AnchorStore` or `ExchangeStore` implementation must extend and pass
`org.apache.spark.resume.api.testkit.AnchorStoreContract` / `ExchangeStoreContract`
(`spark-resume-api`'s test-jar) before it will be merged, in-tree or accepted as a documented
out-of-tree implementation in the README's ecosystem list. This is not a formality — the fencing
and identity-isolation tests in those suites exist because real bugs were found by exactly these
kinds of checks; see `docs/DESIGN.md` §4 and §12 for the specifics.

Any new `SourceFingerprint` implementation must ship BOTH a positive conformance case (identical
source, identical fingerprint) and a negative one (a deliberately mutated source, a different
fingerprint). A fingerprint implementation without the negative case is not considered proven —
see `docs/DESIGN.md` §4, lesson 6, for why a positive-only test suite can pass even when the
fingerprint function is broken.

## Scope discipline

Each module has a stated dependency boundary (see the header comment in each `pom.xml`). If
making a change requires adding a dependency a module's own doc comment says it shouldn't have,
that's a signal the change belongs in a different module, not a reason to loosen the boundary.

## Style

Short, concise doc comments that explain *why*, not just *what* — match the density already in
the codebase. Every non-obvious design decision should be traceable to either a stated invariant
(`docs/DESIGN.md` §7) or an empirical lesson (`docs/DESIGN.md` §4), not asserted without one.

## Before opening a PR

- `mvn install` passes, including tests, for every module you touched.
- New public API surface in `spark-resume-api` gets a doc comment explaining its contract,
  including failure modes — that module is what every third-party implementation compiles
  against, so its documentation is part of the interface.
