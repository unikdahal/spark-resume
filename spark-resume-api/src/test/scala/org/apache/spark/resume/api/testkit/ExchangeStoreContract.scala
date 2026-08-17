package org.apache.spark.resume.api.testkit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.api._

/** Shared conformance suite for any [[ExchangeStore]] implementation. Extend this, implement the
  * three factory hooks below, and every test runs against your implementation for free -- per
  * docs/DESIGN.md §12, a hard requirement for any implementation this project accepts as proven.
  *
  * `conflictingIdentityHandle` is `None`-able: not every backend has the identity-reuse hazard
  * `checkIdentityIsolation` exists to catch (see that method's doc comment on [[ExchangeStore]]).
  * An implementation that legitimately has no such hazard should override it to return `None` --
  * but doing so is a deliberate, documented claim about the backend, not a shortcut to skip
  * writing the hook, and reviewers of a new `ExchangeStore` implementation should treat a `None`
  * here as something to specifically double check, not wave through. */
abstract class ExchangeStoreContract extends AnyFunSuite with Matchers {

  /** A fresh store instance -- called once per test. */
  def newStore(): ExchangeStore

  /** A handle, freshly registered in `store`, that `isFresh` should report `true` for and
    * `reattach` should succeed against. */
  def freshHandle(store: ExchangeStore): ExchangeHandle

  /** A handle whose underlying data `store` should consider superseded/gone --`isFresh` must
    * report `false`. Typically built by registering a handle via the same path as
    * `freshHandle` and then invoking whatever backend-specific supersession this implementation
    * exposes for testing. */
  def staleHandle(store: ExchangeStore): ExchangeHandle

  /** A handle for which `checkIdentityIsolation` should return [[IsolationConflict]], or `None`
    * if this backend genuinely has no such hazard (see class doc comment). */
  def conflictingIdentityHandle(store: ExchangeStore): Option[ExchangeHandle]

  test("a freshly registered handle is fresh") {
    val store = newStore()
    store.isFresh(freshHandle(store)) shouldBe true
  }

  test("a superseded handle is NOT fresh") {
    val store = newStore()
    store.isFresh(staleHandle(store)) shouldBe false
  }

  test("checkIdentityIsolation is IsolationOk for a normal, non-conflicting handle") {
    val store = newStore()
    store.checkIdentityIsolation(freshHandle(store)) shouldBe IsolationOk
  }

  test("checkIdentityIsolation returns IsolationConflict when this backend has the identity-reuse hazard") {
    val store = newStore()
    conflictingIdentityHandle(store) match {
      case Some(h) =>
        store.checkIdentityIsolation(h) match {
          case IsolationConflict(reason) => reason should not be empty
          case IsolationOk => fail("expected IsolationConflict for the deliberately-conflicting handle")
        }
      case None =>
        info("this implementation declares no identity-reuse hazard for this backend -- skipped")
    }
  }

  test("reattach on a fresh handle returns statistics consistent with what was registered") {
    val store = newStore()
    val h = freshHandle(store)
    val result = store.reattach(h)
    result.numMappers should be >= 1
    result.numPartitions should be >= 1
    result.bytesByPartition.length shouldBe result.numPartitions
    result.mapperAttempts.length shouldBe result.numMappers
  }

  test("serializeHandle/deserializeHandle round-trips losslessly") {
    val store = newStore()
    val h = freshHandle(store)
    val payload = store.serializeHandle(h)
    val restored = store.deserializeHandle(payload)
    // Round-tripped handle must behave identically for every operation a caller would perform on
    // it -- checked via effect (isFresh/reattach agree), not via handle equality, since nothing
    // in the ExchangeHandle contract requires structural equality. Compared field-by-field, not
    // via ReattachResult's own case-class equals: its Array fields compare by reference, so two
    // independently-produced (but value-identical) results would never be `==` to each other.
    store.isFresh(restored) shouldBe store.isFresh(h)
    val (r1, r2) = (store.reattach(h), store.reattach(restored))
    r1.numMappers shouldBe r2.numMappers
    r1.numPartitions shouldBe r2.numPartitions
    r1.bytesByPartition.toSeq shouldBe r2.bytesByPartition.toSeq
    r1.rowCount shouldBe r2.rowCount
    r1.mapperAttempts.toSeq shouldBe r2.mapperAttempts.toSeq
  }

  test("deserializeHandle rejects a payload this implementation did not produce") {
    // ExchangeStore.deserializeHandle's contract: "must not silently accept a foreign payload."
    // Feeding it bytes no real serializeHandle call ever produced must throw, not return a handle
    // that only fails later (or, worse, one that happens to alias a real handle by coincidence).
    val store = newStore()
    a[RuntimeException] should be thrownBy store.deserializeHandle("not a real payload, ever".getBytes("UTF-8"))
  }

  test("handleKind is non-empty and stable across calls") {
    val store = newStore()
    store.handleKind should not be empty
    store.handleKind shouldBe store.handleKind
  }
}
