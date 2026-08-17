# spark-resume-redis

The first real, cross-process `AnchorStore` implementation (`docs/DESIGN.md` §14 Phase 2) —
everything the in-memory reference store in `spark-resume-api` explicitly is not: durable, and
visible to a genuinely different driver process, which is the entire point of this project.

## Running the tests — needs a REAL Redis

This module's tests run the full `AnchorStoreContract` conformance suite (`docs/DESIGN.md` §12)
against an actual Redis server, not a mock or an embedded fake — including the 16-thread
concurrent-writer test, which is exactly the scenario a non-atomic implementation fails only under
genuine concurrent pressure. That means `mvn test` here (and therefore `mvn clean install` at the
repo root) needs a Redis reachable at `REDIS_HOST`/`REDIS_PORT` (default `localhost`/`16379`), and
**fails loudly, not silently skips**, if there isn't one — verified directly: stopping the
container mid-run turns every test red with a real `JedisConnectionException`, not a quietly
skipped suite. Start one:

```
podman run -d --name spark-resume-redis-test -p 16379:6379 redis:7-alpine
```

(or the `docker` equivalent). Stop it with `podman rm -f spark-resume-redis-test` when done.

## What makes `putAnchor` and `acquireGeneration` actually safe under concurrency

Both are atomic on the **Redis server**, not this client:

- `acquireGeneration` is Redis's own `INCR` — a single command, already atomic by Redis's
  one-command-at-a-time execution model.
- `putAnchor` is a small Lua script, evaluated server-side via `EVAL`: the compare-current-
  generation check and the write happen as ONE atomic step. A naive client-side "GET the current
  generation, compare, then RPUSH if it matches" has a real gap there — two concurrent callers
  could both read the same current generation before either writes, and both would (wrongly)
  proceed. See `RedisAnchorStore`'s doc comment for the exact script.

A single `Jedis` connection is not safe to share across concurrent callers (the Redis wire
protocol is request/response on one connection), so `RedisAnchorStore` is backed by a
`JedisPool` and checks out a connection per call — proven safe under the conformance testkit's
real-thread concurrency test, not just documented as safe.

## Wire format

`Anchor`s are encoded via `AnchorCodec` — an explicit, versioned, length-prefixed binary format,
not Java serialization (whose wire shape is a JVM implementation detail, not a compatibility
commitment). `Anchor.schemaVersion` is written first and decoded on, so a future format change is
additive. `AnchorCodecSpec` round-trips every field and compares by CONTENT, not by reference —
`Anchor`'s generated `equals` is reference equality on its `Array[Byte]`/`Array[Long]` fields, so a
naive `decoded shouldBe original` would compare the wrong thing.

## What this does NOT prove

No TTL/eviction policy, no cluster-mode testing (single-node Redis only), no `AUTH`/TLS
configuration surface — this proves the fencing semantics are correct against a real server, not
that this is a hardened production Redis deployment.
