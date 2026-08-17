# spark-resume-fs

A second, independent `ExchangeStore` implementation — filesystem-backed, real files, zero
external infrastructure. Not built to prove out a real disaggregated shuffle service
(`spark-resume-celeborn` already does that); its purpose is narrower and answers a different
question: **is `ExchangeStoreContract` actually satisfiable, completely, by someone who did not
write it, using only the published testkit?**

## The result

Yes, as written, no testkit changes needed. 16/16 tests pass: `ExchangeStoreContract`'s 8 (with
`reattachSupported` left at its default `true`), plus a codec spec (5, mirroring
`spark-resume-celeborn`'s `CelebornHandleCodecSpec`) and 3 direct tests for behavior the shared
contract has no vocabulary for. This is a genuinely different outcome than the last time a new
`ExchangeStore` implementation was built here: writing `CelebornExchangeStore` found a real
testkit defect (`ExchangeStoreContract` unconditionally required `reattach` to succeed, making it
unsatisfiable by an honest Tier 3 backend — fixed with the `reattachSupported` hook). This module
found none — evidence the fix held, not just an assumption that it would.

## Why `reattach` here matters more than it looks

Every other real `ExchangeStore` in this repo (`spark-resume-celeborn`) declares
`reattachSupported = false` — a genuine, checked, permanent backend limitation, not a shortcut.
That means until this module, `ExchangeStoreContract`'s reattach-SUCCESS path (the branch that
actually calls `store.reattach` and asserts on the result) had only ever been exercised by
`InMemoryExchangeStore`, the reference implementation the contract was originally written
against — a conformance claim tested only against its own author isn't proven for anyone else.
`FsExchangeStore.reattach` is real: it reads an actual manifest file and an actual data file back
off disk and returns statistics parsed from them, not fixture echoes.

## On-disk layout

```
baseDir/<slotId>/current             one line: the CURRENT generation (Long)
baseDir/<slotId>/gen-<N>.manifest    numMappers / numPartitions / bytesByPartition (csv) /
                                      rowCount / mapperAttempts (csv), one field per line
baseDir/<slotId>/gen-<N>.data        the actual placeholder "shuffle bytes" for generation N
```

`current` is written via `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)` from a temp file — the
same "real CAS primitive, not a read-then-write race" property this project's other stores get
from Redis `INCR` / a Lua script / Celeborn's own master, done here with a filesystem rename
instead. `isFresh(handle)` compares `handle.generation` against `current`'s live value.

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
- `reattach` independently detects a manifest/data size mismatch (the data file truncated after
  the fact) as corruption — `isFresh` alone can't catch this (the generation marker is untouched),
  so this is `reattach`'s own defense, checked directly, not inferred.

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
