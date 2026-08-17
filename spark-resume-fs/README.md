# spark-resume-fs

A second, independent `ExchangeStore` implementation — filesystem-backed, real files, zero
external infrastructure. Originally built to answer a narrower question than what it's grown into:
**is `ExchangeStoreContract` actually satisfiable, completely, by someone who did not write it,
using only the published testkit?** As of Phase 4, it is also this repo's real proof target for
actual execution-skipping (`spark-resume-spark-3.5`'s `ExecutionSkipRule`/`SkippedShuffleRDD`) —
`spark-resume-celeborn` cannot serve that role, since its `reattach`/`store`/`readPartition` are
all the documented Tier 3 gap.

## The testkit result

Yes, as written, no testkit changes needed. 18/18 tests pass: `ExchangeStoreContract`'s 9
(including `store`/`readPartition`'s round-trip test, with `reattachSupported` left at its default
`true`), plus a codec spec (5, mirroring `spark-resume-celeborn`'s `CelebornHandleCodecSpec`) and 4
direct tests for behavior the shared contract has no vocabulary for. This is a genuinely different
outcome than the last time a new `ExchangeStore` implementation was built here: writing
`CelebornExchangeStore` found a real testkit defect (`ExchangeStoreContract` unconditionally
required `reattach` to succeed, making it unsatisfiable by an honest Tier 3 backend — fixed with
the `reattachSupported` hook). This module found none — evidence the fix held, not just an
assumption that it would.

## Why `reattach`/`store`/`readPartition` here matter more than they look

Every other real `ExchangeStore` in this repo (`spark-resume-celeborn`) declares
`reattachSupported = false` — a genuine, checked, permanent backend limitation, not a shortcut,
and that flag now also gates `store`/`readPartition` (see `ExchangeStoreContract`'s doc comment
on why one flag covers all three: the same Tier 3 capability requirement). That means until this
module, none of the contract's success paths — `reattach`, or the newer `store`/`readPartition`
producer/consumer pair added alongside real execution-skipping — had ever been exercised by
anything but `InMemoryExchangeStore`, the reference implementation the contract was originally
written against. `FsExchangeStore.reattach`/`store`/`readPartition` are real: actual files, read
and written per-partition, not fixture echoes — and (Phase 4) `spark-resume-spark-3.5`'s
`StageCaptureListener`/`ExecutionSkipRule` use exactly this store to prove a real Spark stage can
be genuinely skipped, not just admitted on paper. See `spark-resume-integration`'s `skip` scenario
for the full cross-process version of that proof.

## On-disk layout

```
baseDir/<slotId>/current                one line: the CURRENT generation (Long)
baseDir/<slotId>/gen-<N>.manifest       numMappers / numPartitions / bytesByPartition (csv) /
                                         rowCount / mapperAttempts (csv), one field per line
baseDir/<slotId>/gen-<N>.part-<i>.data  the actual bytes for partition `i` of generation `N` --
                                         per-partition-addressable (not one combined blob), so
                                         readPartition(handle, i) reads ONLY partition i, the way
                                         a real execution-skip RDD's compute(i) needs to on the
                                         executor that will consume it.
```

`current` is written via `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)` from a temp file — the
same "real CAS primitive, not a read-then-write race" property this project's other stores get
from Redis `INCR` / a Lua script / Celeborn's own master, done here with a filesystem rename
instead. `isFresh(handle)` compares `handle.generation` against `current`'s live value.
`reattach`'s corruption check is genuinely PER-PARTITION now (each partition file's actual size is
checked against what the manifest claims for THAT partition), not just an aggregate sum.

## What's tested beyond the shared contract

- `staleHandle` is a REAL supersession here (write generation 1, write again to bump the same
  slot to generation 2, hand back generation 1's now-stale handle) — a stronger, more realistic
  test than `spark-resume-celeborn`'s deliberately narrower "never registered at all" scoping
  choice (a real, disclosed decision there, not a gap; this backend just makes the stronger claim
  trivial to drive directly, so there's no reason not to).
- `reattach` on a handle whose manifest was never written throws `NoSuchElementException` — a real
  bug/precondition-violation signal, not the Tier 3 `UnsupportedOperationException` (this store
  genuinely supports reattach; a missing manifest here means a caller skipped `SafeReattach`, not
  a backend capability gap).
- `reattach` independently detects a manifest/data size mismatch as corruption, PER PARTITION (two
  partitions written, only one truncated) — `isFresh` alone can't catch this (the generation
  marker is untouched), so this is `reattach`'s own defense, checked directly, not inferred, and
  proven to catch a truncation anywhere, not just an aggregate-sum mismatch a compensating error
  elsewhere could mask.
- `readPartition` reads back the exact bytes for the partition asked, including a genuinely empty
  partition (a real, legitimate case, not a missing one), and rejects an out-of-range index.

## Running the tests

```
mvn -pl spark-resume-api,spark-resume-fs -am test
```

No external infrastructure needed — every test uses a real local temp directory
(`Files.createTempDirectory`), cleaned up by the OS, not this module.

## Why this module needs so little

Production code is `FsHandle`/`FsHandleCodec`/`FsExchangeStore` — nothing else, no dependency
beyond `spark-resume-api` and `java.nio.file`. A store that only ever needs local disk access has
no business depending on anything more.
