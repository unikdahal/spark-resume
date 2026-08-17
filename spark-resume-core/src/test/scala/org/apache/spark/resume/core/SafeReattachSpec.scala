package org.apache.spark.resume.core

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.api._
import org.apache.spark.resume.api.memory.{InMemoryExchangeStore, InMemoryHandle}

/** Wraps a real store and counts calls to `reattach` -- so a test can assert not just the
  * returned outcome but that `reattach` was NEVER actually invoked on a refused handle, which is
  * the actual property `SafeReattach` exists to guarantee (see its class doc comment). */
class CountingExchangeStore(delegate: ExchangeStore) extends ExchangeStore {
  val reattachCalls = new AtomicInteger(0)
  override def handleKind: String = delegate.handleKind
  override def serializeHandle(handle: ExchangeHandle): Array[Byte] = delegate.serializeHandle(handle)
  override def deserializeHandle(payload: Array[Byte]): ExchangeHandle = delegate.deserializeHandle(payload)
  override def isFresh(handle: ExchangeHandle): Boolean = delegate.isFresh(handle)
  override def checkIdentityIsolation(handle: ExchangeHandle): IsolationResult =
    delegate.checkIdentityIsolation(handle)
  override def reattach(handle: ExchangeHandle): ReattachResult = {
    reattachCalls.incrementAndGet()
    delegate.reattach(handle)
  }
}

class SafeReattachSpec extends AnyFunSuite with Matchers {

  private def freshResult = ReattachResult(2, 3, Array(10L, 20L, 30L), 100L, Array(0, 0))

  test("a fresh, isolation-ok handle reattaches successfully") {
    val inner = new InMemoryExchangeStore
    val handle: InMemoryHandle = inner.put("h1", freshResult)
    val spy = new CountingExchangeStore(inner)

    SafeReattach.attempt(spy, handle) match {
      case Reattached(result) =>
        result.numMappers shouldBe 2
        result.rowCount shouldBe 100L
      case other => fail(s"expected Reattached, got $other")
    }
    spy.reattachCalls.get() shouldBe 1
  }

  test("a stale handle is refused BEFORE reattach is ever called") {
    val inner = new InMemoryExchangeStore
    val handle = inner.put("h2", freshResult)
    inner.markSuperseded(handle)
    val spy = new CountingExchangeStore(inner)

    SafeReattach.attempt(spy, handle) shouldBe RefusedStale
    spy.reattachCalls.get() shouldBe 0
  }

  test("an identity-conflicting handle is refused BEFORE reattach is ever called -- the A-6 guarantee") {
    val inner = new InMemoryExchangeStore
    val handle = inner.put("h3", freshResult)
    inner.markIdentityConflict(handle)
    val spy = new CountingExchangeStore(inner)

    SafeReattach.attempt(spy, handle) match {
      case RefusedIsolationConflict(reason) => reason should not be empty
      case other => fail(s"expected RefusedIsolationConflict, got $other")
    }
    spy.reattachCalls.get() shouldBe 0
  }

  test("freshness is checked before identity isolation -- a stale AND conflicting handle is reported as stale") {
    val inner = new InMemoryExchangeStore
    val handle = inner.put("h4", freshResult)
    inner.markSuperseded(handle)
    inner.markIdentityConflict(handle)
    val spy = new CountingExchangeStore(inner)

    SafeReattach.attempt(spy, handle) shouldBe RefusedStale
    spy.reattachCalls.get() shouldBe 0
  }
}
