package org.apache.spark.resume.redis

import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Arrays => JArrays}

import scala.collection.JavaConverters._

import redis.clients.jedis.{Jedis, JedisPool}

import org.apache.spark.resume.api.{Anchor, AnchorStore}

/** The first real, cross-process [[AnchorStore]] implementation (docs/DESIGN.md §14 Phase 2) --
  * everything the in-memory reference implementation in `spark-resume-api` explicitly is NOT
  * (durable, visible to a genuinely different driver process).
  *
  * Both fencing operations this project's fail-closed correctness depends on are made atomic on
  * the REDIS SERVER, not this client:
  *   - [[acquireGeneration]] is Redis's own `INCR` -- a single command, already atomic by Redis's
  *     one-command-at-a-time execution model; no read-then-write race is possible here at all.
  *   - [[putAnchor]] is a small Lua script (`RedisAnchorStore.PutAnchorScript`), evaluated
  *     server-side via `EVAL`: the compare-current-generation check and the write happen as ONE
  *     atomic server-side step, not two round-trips a concurrent writer could interleave with. A
  *     naive client-side "GET the current generation, compare, then RPUSH if it matches" would
  *     have exactly that gap -- two concurrent callers could both read the same current
  *     generation before either writes, and both would then (wrongly) proceed. This is precisely
  *     the race [[org.apache.spark.resume.api.testkit.AnchorStoreContract]]'s concurrent-writer
  *     test exists to catch.
  *
  * One `Jedis` connection is NOT safe to share across concurrent callers (the Redis wire protocol
  * is request/response on a single connection; concurrent use corrupts the request stream) --
  * this store is backed by a `JedisPool` and checks out a connection per call specifically so it
  * is safe under the SAME concurrent-caller pressure the conformance testkit exercises with real
  * threads, not just documented as safe.
  *
  * Anchor bytes are encoded via [[AnchorCodec]] -- an explicit, versioned wire format, not Java
  * serialization (see that object's doc comment for why).
  *
  * `close()` (`java.io.Closeable`) closes the underlying `JedisPool` only when THIS store created
  * it (the `(host, port[, keyPrefix])` constructors) -- a `JedisPool` passed in directly is
  * assumed to be owned and lifecycle-managed by the caller (it may be shared with other code),
  * and this store must never close a resource it didn't create. The `(host, port)` constructors
  * exist specifically so a caller who doesn't want to think about pool lifecycle at all can still
  * get one back via `close()` instead of leaking a pool with no way to shut it down. */
final class RedisAnchorStore private (pool: JedisPool, keyPrefix: String, ownsPool: Boolean)
    extends AnchorStore with java.io.Closeable {

  def this(pool: JedisPool, keyPrefix: String) = this(pool, keyPrefix, ownsPool = false)
  def this(pool: JedisPool) = this(pool, "spark-resume", ownsPool = false)
  def this(host: String, port: Int, keyPrefix: String) = this(new JedisPool(host, port), keyPrefix, ownsPool = true)
  def this(host: String, port: Int) = this(host, port, "spark-resume")

  override def close(): Unit = if (ownsPool) pool.close()

  private def genKey(queryId: String): String = s"$keyPrefix:gen:$queryId"
  private def anchorsKey(queryId: String): String = s"$keyPrefix:anchors:$queryId"

  override def acquireGeneration(queryId: String): Long =
    withJedis(_.incr(genKey(queryId)))

  override def putAnchor(generation: Long, anchor: Anchor): Boolean = withJedis { jedis =>
    val keys = JArrays.asList(
      genKey(anchor.queryId).getBytes(UTF_8),
      anchorsKey(anchor.queryId).getBytes(UTF_8))
    val args = JArrays.asList(
      generation.toString.getBytes(UTF_8),
      AnchorCodec.encode(anchor))
    jedis.eval(RedisAnchorStore.PutAnchorScriptBytes, keys, args) match {
      case n: java.lang.Long => n.longValue() == 1L
      case other =>
        // Defensive, not expected: a well-formed script always returns a Lua integer, which
        // Jedis always maps to java.lang.Long -- if that ever stops being true (a Redis/Jedis
        // version change), fail closed (A-2) rather than silently miscount a write as successful.
        throw new IllegalStateException(s"RedisAnchorStore: unexpected EVAL result type: $other")
    }
  }

  override def loadAnchors(queryId: String): Seq[Anchor] = withJedis { jedis =>
    jedis.lrange(anchorsKey(queryId).getBytes(UTF_8), 0, -1).asScala.toSeq.map(AnchorCodec.decode)
  }

  private def withJedis[T](f: Jedis => T): T = {
    val jedis = pool.getResource
    try f(jedis) finally jedis.close() // returns the connection to the pool, does not disconnect
  }
}

object RedisAnchorStore {

  /** `KEYS[1]` = the generation-fence key, `KEYS[2]` = the anchors list key, `ARGV[1]` = the
    * generation this write is gated on, `ARGV[2]` = the encoded anchor bytes. Returns Lua `1` on
    * a successful write, `0` if `ARGV[1]` is not the CURRENT value at `KEYS[1]` -- mirrors
    * [[AnchorStore.putAnchor]]'s documented `Boolean` contract exactly: a missing generation key
    * (nothing ever called `acquireGeneration` for this `queryId`) is treated as current value
    * `'0'`, matching [[org.apache.spark.resume.api.testkit.AnchorStoreContract]]'s documented `0`
    * sentinel for "never acquired." */
  private[redis] val PutAnchorScript: String =
    """|local current = redis.call('GET', KEYS[1])
       |if current == false then
       |  current = '0'
       |end
       |if current ~= ARGV[1] then
       |  return 0
       |end
       |redis.call('RPUSH', KEYS[2], ARGV[2])
       |return 1""".stripMargin

  private val PutAnchorScriptBytes: Array[Byte] = PutAnchorScript.getBytes(UTF_8)
}
