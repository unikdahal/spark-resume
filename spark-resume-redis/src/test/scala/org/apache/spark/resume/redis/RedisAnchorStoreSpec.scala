package org.apache.spark.resume.redis

import java.util.UUID

import redis.clients.jedis.JedisPool

import org.apache.spark.resume.api.AnchorStore
import org.apache.spark.resume.api.testkit.AnchorStoreContract

/** Runs the full [[AnchorStoreContract]] conformance suite against a REAL Redis instance -- not a
  * mock, not an embedded fake. Needs a Redis reachable at `REDIS_HOST`/`REDIS_PORT` (default
  * `localhost`/`16379`, matching this repo's own dev container -- see `README.md` for how to
  * start one). `newStore()` gets a fresh, random key prefix per test (a real Redis server, unlike
  * the in-memory reference store, is NOT wiped between tests -- this is what keeps tests from
  * seeing each other's keys without needing a `FLUSHDB` that would also be unsafe against a
  * shared/production Redis). */
class RedisAnchorStoreSpec extends AnchorStoreContract {

  private val host = sys.env.getOrElse("REDIS_HOST", "localhost")
  private val port = sys.env.getOrElse("REDIS_PORT", "16379").toInt
  private val pool = new JedisPool(host, port)

  override def newStore(): AnchorStore =
    new RedisAnchorStore(pool, keyPrefix = s"test-${UUID.randomUUID()}")
}
