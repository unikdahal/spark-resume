package org.apache.spark.resume.api.memory

import org.apache.spark.resume.api._
import org.apache.spark.resume.api.testkit.ExchangeStoreContract

class InMemoryExchangeStoreSpec extends ExchangeStoreContract {
  override def newStore(): ExchangeStore = new InMemoryExchangeStore

  private def register(store: ExchangeStore, id: String): InMemoryHandle =
    store.asInstanceOf[InMemoryExchangeStore].put(
      id,
      ReattachResult(
        numMappers = 2,
        numPartitions = 3,
        bytesByPartition = Array(10L, 20L, 30L),
        rowCount = 100L,
        mapperAttempts = Array(0, 0)))

  override def freshHandle(store: ExchangeStore): ExchangeHandle = register(store, "fresh")

  override def staleHandle(store: ExchangeStore): ExchangeHandle = {
    val h = register(store, "stale")
    store.asInstanceOf[InMemoryExchangeStore].markSuperseded(h)
    h
  }

  override def conflictingIdentityHandle(store: ExchangeStore): Option[ExchangeHandle] = {
    val h = register(store, "conflict")
    store.asInstanceOf[InMemoryExchangeStore].markIdentityConflict(h)
    Some(h)
  }
}
