# spark-resume-celeborn

The first real, cross-process `ExchangeStore` implementation (`docs/DESIGN.md` §14 Phase 3):
Apache Celeborn-backed, against a vanilla, published Celeborn release (`0.7.0`) — not a private
fork.

## What's real here, and what isn't

`handleKind`, `serializeHandle`/`deserializeHandle`, `isFresh`, and `checkIdentityIsolation` are
real, tested against a real Celeborn master and worker (`CelebornExchangeStoreSpec`, extending the
same `ExchangeStoreContract` every `AnchorStore`/`ExchangeStore` implementation in this repo is
required to pass — 8 tests). `isFresh` queries the master's own admin REST API
(`ShuffleApi.getShuffles()`) live, not a cache; `checkIdentityIsolation` is a real, enforced check
against this project's own disclosed hazard (see below).

**`reattach` is not implemented.** It throws `UnsupportedOperationException` naming the exact gap.
This is `docs/DESIGN.md` §8's Tier 3 exactly as specified: *"a documented backend-patch
requirement... a property of the shuffle service, not of Spark."* Verified by checking vanilla
Celeborn `0.7.0`'s actual public client API (`ShuffleClient`, `LifecycleManager`), not assumed:
`readPartition`/`registerMapPartitionTask` are scoped to the `LifecycleManager` that registered the
shuffle. There is no public method anywhere in the client API to read a shuffle's committed
partition locations under a *different* application's identity. `spark-resume-core`'s
`SafeReattach.attempt` converts that exception into a structured `RefusedUnsupported` outcome, so
a caller doing everything right gets a refusal, not a raw throw — proven end to end against this
real store in `SafeReattachIntegrationSpec`, not just documented.

## A real safety check, not just a disclosed hazard

A resuming driver launched under the same Celeborn `appUniqueId` as the run that produced an
anchor risks silently colliding with that run's own wire state rather than cleanly reading its
committed output — Celeborn scopes a shuffle's registration to the `appUniqueId` that created it,
so reusing that id is a self-inflicted identity collision, not a resumption. `checkIdentityIsolation`
here is a real, enforced, tested check for exactly that: it compares the anchor's producing
`appUniqueId` against the resuming driver's own, and refuses with `IsolationConflict` on a match.

## Running the tests — needs a real Celeborn cluster

```
./run-celeborn-tests.sh
```

Downloads the official Apache Celeborn `0.7.0` binary distribution (once; cached under
`../.vendor/`, not committed), starts a local single-master/single-worker cluster from it, runs
this module's tests, and tears the cluster down on exit. Reproduced clean across 2 consecutive
runs from a cold standup. No private/patched Celeborn build is used or referenced anywhere in this
module — every dependency is a real, published Apache Celeborn artifact.

`CelebornExchangeStoreSpec`'s `freshHandle` drives an actual shuffle registration through
Celeborn's real, engine-agnostic client API (`LifecycleManager`/`ShuffleClient` — the same public
API Flink/MR use, not a Spark-specific shim), confirmed against a live master, not assumed:
`ShuffleApi.getShuffles()` was checked directly and returns ids in `"<appUniqueId>-<shuffleId>"`
form. `staleHandle` is deliberately a shuffle id that was *never* registered, rather than a
registered-then-superseded one — a real, disclosed scoping choice: this proves `isFresh`'s
negative path for "never existed," not "existed then expired," since chasing this specific
master version's exact supersession/expiry timing wasn't needed to answer `isFresh` correctly
either way.

## Why this module needs so little

Production code depends on `celeborn-openapi-client` only — the master's admin REST client. It
never depends on `celeborn-client-spark-3-shaded` (the shuffle data-plane client a real Spark
application already has on its own classpath for its actual shuffle manager); that dependency
exists at **test scope only**, to drive one real shuffle registration for the conformance
fixtures. A store that only ever answers metadata questions has no business bundling a data-plane
client it never calls.

## A testkit finding along the way

`ExchangeStoreContract`, as originally written, unconditionally required `reattach` to succeed in
two of its eight tests — unsatisfiable by an honest Tier 3 implementation. Fixed by adding a
`reattachSupported: Boolean = true` hook (default preserves existing behavior for every other
implementation), mirroring the existing `conflictingIdentityHandle: Option[...]` pattern for "not
every backend has this hazard." A `false` override is, like that one, a deliberate documented claim
about the backend, not a shortcut — see the testkit's own doc comment.
