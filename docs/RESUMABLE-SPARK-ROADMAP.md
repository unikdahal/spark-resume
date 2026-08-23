# Resumable Spark driver: authoritative architecture and delivery roadmap

Status: working contract. This document supersedes conflicting implementation claims in the POC
READMEs. The detailed reasoning remains in `LLD-resumable-spark-driver.md`; this file says which
mechanism is intended to ship and what evidence is required before calling it correct.

## Product contract

A restarted batch driver may reuse a completed unit of work only when all of these facts are
durable and can be revalidated:

1. the unit's semantic identity, including the exact resolved input versions and query constants;
2. the complete output catalog and runtime statistics;
3. an atomic completion marker tying the identity, catalog, and statistics to one fenced attempt;
4. backend proof that the referenced bytes still exist and belong to that identity domain; and
5. enough engine state to install the unit as already materialized without changing downstream
   planning or results.

The guarantee is therefore:

> After a driver crash, do not recompute any previously completed, durably anchored stage that
> passes every admission check. Recompute on every miss, ambiguity, unsupported plan shape,
> expired output, partial anchor, or validation failure.

This does not promise preservation of in-flight tasks. It also does not make arbitrary external
side effects exactly-once. A sink must provide an atomic or idempotent commit protocol; otherwise
the write is not resumable and must be rejected.

## One production data path

The production path is in-place shuffle adoption:

```text
producer Spark driver
  -> Celeborn commits shuffle files and catalog
  -> Spark receives stage success and stable runtime statistics
  -> coordinator durably commits one fenced stage anchor

replacement Spark driver
  -> rebuilds and fingerprints the query naturally
  -> validates input identity, determinism, anchor fence, and Celeborn data
  -> installs Celeborn's catalog into the replacement LifecycleManager
  -> seeds Spark's shuffle metadata and creates an already-materialized AQE stage
  -> continues downstream without submitting the adopted stage's map tasks
```

`spark-resume`'s `ExecutionSkipRule` plus `FsExchangeStore` remains a useful semantic oracle and
connector-neutral test backend. It is not the scalable Celeborn implementation: it executes a
second read of completed shuffle output, encodes rows through the driver, and stores another copy.
Production must leave bytes in Celeborn and adopt their catalog in place.

## Component ownership

| Component | Owns | Must not own |
|---|---|---|
| Spark | stage lifecycle hook, materialized-stage installation, MapOutputTracker state, AQE statistics, exchange reuse | Celeborn file metadata or application retention |
| Celeborn | durable shuffle identity/catalog, adoption into a fresh LifecycleManager, exact existence validation, retention and cleanup | SQL semantic equivalence |
| Iceberg (or connector adapter) | immutable resolved read identity or read-version pinning | Spark stage lifecycle |
| Resume coordinator/library | semantic digest, determinism closure, fencing, durable anchors, admission, observability | copying production shuffle bytes through the driver |
| Transactional sink adapter | commit identity, recovery, duplicate suppression | claiming generic write safety |

## Correctness invariants

- Fail closed: any exception or unknown state means normal Spark execution.
- No identity by `hashCode`, object identity, unstable `toString`, stage ID, or traversal position.
- Fingerprints use the state the producing task actually read, not metadata refreshed after it ran.
- An anchor becomes visible only after all bytes/catalog/statistics are complete; partial anchors
  are never admissible.
- Adoption is atomic with respect to a replacement driver's writers and is protected from a
  delayed old driver by a monotonically increasing lease/fence.
- Celeborn's data namespace uses a stable logical resume-application ID across driver attempts;
  each driver also has a distinct monotonically fenced epoch. A new Spark application ID alone
  cannot address files written under the producer's Celeborn shuffle key, while reusing an
  unfenced Celeborn application ID permits zombie-driver corruption.
- Celeborn validates exact referenced files/epochs, not merely that their worker ports are open.
- Restored runtime statistics include partition bytes, total data size, and row count before AQE
  replans. `MapOutputStatistics` alone is insufficient.
- Partition count, mapper attempts, serializer/schema, shuffle dependency semantics, and
  partitioning all match before executor reads are possible.
- A rejected adoption leaves no partially installed Spark or Celeborn state.
- Cleanup cannot reclaim adopted data while a live resumed application holds its lease.

## Current evidence and blocking gaps

| Area | Existing evidence | Blocking gap |
|---|---|---|
| Spark AQE skip | `resume-poc-e2e` skips a real Celeborn stage across JVMs | Hook API is broad/global, install is not transactional, and upstream-style tests are missing |
| AQE correctness | Real bug found and fixed by restoring `dataSize` and row-count metrics | More AQE rules, exchange reuse, skew/coalescing, subqueries, and concurrent queries need coverage |
| Celeborn adoption | Catalog seeding and mapper attempts read real bytes after restart | `confirmAlive` checks TCP reachability, not exact file freshness; lifetime and rollback are incomplete |
| Fingerprints | Stage Merkle digest and Iceberg mutation tests exist | Generic fallback can still false-admit; unpinned Iceberg capture can observe a newer snapshot |
| Durable metadata | Redis generation fencing exists in `spark-resume` | Stage anchor commit must be integrated with the actual Spark/Celeborn stage callback and lease model |
| Library skip oracle | Cross-process filesystem test proves correct rows and fewer tasks | It copies rows through the driver and is intentionally not the production backend |
| Writes and side inputs | DPP, broadcast, result, constants, and DSv2 write spikes exist | They are separate hooks, not one atomic query-generation protocol; write recovery needs sink contracts |

## Delivery order and gates

### 1. Make shuffle adoption trustworthy

- Replace port probing with a Celeborn-owned exact catalog/file validation operation.
- Make catalog adoption validate first and publish atomically, with rollback on failure.
- Define application identity, lease renewal, old-driver fencing, retention extension, and cleanup.
- Replace the POC's old "different appUniqueId means isolation" rule. Recovery requires the same
  logical Celeborn namespace plus a newer fenced driver epoch; identity isolation is provided by
  the epoch, not by changing the namespace used to construct worker shuffle keys.
- Unit-test duplicate adoption, conflicting adoption, truncated/missing files, mapper-attempt
  mismatch, partial catalogs, concurrent registration, and cleanup races.

Gate: no Spark stage is marked materialized unless Celeborn has atomically accepted and validated
the exact anchor.

### 2. Make Spark stage restoration native and minimal

- Replace the process-global hook slots with a session/execution-scoped extension contract.
- Return a typed restored-stage descriptor containing statistics and validation metadata, rather
  than letting an external hook mutate arbitrary Spark objects.
- Install the restored stage through the same AQE stage cache and result state used by a normally
  completed stage. Restore all statistics before the next optimization pass.
- Add Spark suites for no-hook behavior, accepted adoption, every rejection path, exchange reuse,
  nested AQE/subqueries, skew/coalescing, cancellation, concurrent queries, and hook exceptions.

Gate: correct results, identical final-plan semantics, zero submitted tasks for each adopted
stage, and unchanged behavior when the feature is absent or refuses.

### 3. Close semantic identity

- Eliminate permissive generic scan fallback from admission. Unknown sources are unsupported.
- Fingerprint or pin the resolved Iceberg snapshot before execution; never query a live table
  handle from a post-execution listener.
- Add connector contracts for immutable version tokens or replayable planned splits.
- Persist query constants and apply the determinism closure to each stage digest.

Gate: mutation, time travel, branch movement, DPP, nondeterministic expressions, configuration,
timezone, UDF/JAR, and serializer changes all either produce distinct identities or refuse.

### 4. Make the protocol crash-consistent

- Persist immutable anchor payloads, then atomically publish the completion record.
- Inject crashes before and after every protocol transition on producer and replacement drivers.
- Re-run each scenario with Redis loss, Celeborn worker loss/restart, expired data, corrupt anchor,
  stale generation, and delayed zombie-driver actions.

Gate: every crash point yields either safe adoption with zero recomputation of anchored stages or
ordinary recomputation. No crash point yields a wrong result or a partially installed adoption.

### 5. Broaden beyond shuffle stages

DPP, broadcast, result reuse, and writes land only after the shuffle protocol is stable. Each is
a separate capability with an explicit anchor schema and admission contract. Write recovery stays
off by default and requires an adapter declaring atomic/idempotent commit behavior.

## Upstream shape

Do not propose the whole project as one Spark patch. Prepare independent contributions:

1. Celeborn durable catalog export/adopt/validate and retention semantics;
2. a small Spark AQE restored-stage extension with no Celeborn dependency;
3. an external coordinator/reference implementation and conformance testkit;
4. connector-specific identity adapters, beginning with Iceberg;
5. optional side-input and transactional-write capabilities after the base protocol is proven.

Each upstream change must be independently useful, disabled by default, have no behavior change
without an implementation installed, and carry failure-injection evidence rather than only a
successful demo.
