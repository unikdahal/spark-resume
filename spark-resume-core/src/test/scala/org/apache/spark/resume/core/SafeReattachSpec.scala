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

/** Delegates everything except `reattach`, which always throws `toThrow` -- for testing what
  * `SafeReattach.attempt` does with a store whose `reattach` is a documented capability gap (or,
  * separately, a genuine bug/transient failure). */
class ThrowingReattachStore(delegate: ExchangeStore, toThrow: => Throwable) extends ExchangeStore {
  override def handleKind: String = delegate.handleKind
  override def serializeHandle(handle: ExchangeHandle): Array[Byte] = delegate.serializeHandle(handle)
  override def deserializeHandle(payload: Array[Byte]): ExchangeHandle = delegate.deserializeHandle(payload)
  override def isFresh(handle: ExchangeHandle): Boolean = delegate.isFresh(handle)
  override def checkIdentityIsolation(handle: ExchangeHandle): IsolationResult =
    delegate.checkIdentityIsolation(handle)
  override def reattach(handle: ExchangeHandle): ReattachResult = throw toThrow
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

  test("a fresh, isolation-ok handle whose store documents reattach as unsupported (Tier 3 capability gap) -> RefusedUnsupported, not a raw throw") {
    val inner = new InMemoryExchangeStore
    val handle = inner.put("h5", freshResult)
    val throwing = new ThrowingReattachStore(inner, new UnsupportedOperationException("no cross-application read path"))

    SafeReattach.attempt(throwing, handle) match {
      case RefusedUnsupported(reason) => reason should include("cross-application")
      case other => fail(s"expected RefusedUnsupported, got $other")
    }
  }

  test("a fresh, isolation-ok handle whose store's reattach throws something OTHER than UnsupportedOperationException -> propagates, is NOT swallowed into a refusal") {
    // A real bug or a transient backend failure (a network timeout, a corrupt record) must not be
    // folded into the same "this is fine, business as usual" refusal shape as a documented
    // capability gap -- only UnsupportedOperationException gets that treatment.
    val inner = new InMemoryExchangeStore
    val handle = inner.put("h6", freshResult)
    val throwing = new ThrowingReattachStore(inner, new RuntimeException("transient backend failure"))

    a[RuntimeException] should be thrownBy SafeReattach.attempt(throwing, handle)
  }
}
