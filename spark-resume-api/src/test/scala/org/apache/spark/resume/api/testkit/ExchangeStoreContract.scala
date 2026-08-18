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

  /** `true` (the default) if this implementation's `reattach` is expected to succeed against a
    * fresh handle -- `false` for a backend whose `reattach` is a documented, permanent
    * `UnsupportedOperationException` because re-registering an existing shuffle's committed
    * locations under a new application identity is not something the backend's own public client
    * API exposes (docs/DESIGN.md §8's Tier 3: "a documented backend-patch requirement," a
    * property of the BACKEND, not of this implementation's code -- see e.g.
    * `spark-resume-celeborn`'s `CelebornExchangeStore`, whose `handleKind`/`isFresh`/
    * `checkIdentityIsolation` are real against a real cluster while `reattach` is not, because
    * vanilla Apache Celeborn's client API was checked and confirmed to have no cross-application
    * shuffle-read capability). Overriding this to `false` is, like `conflictingIdentityHandle`
    * returning `None`, a deliberate documented claim about the backend, not a shortcut -- found
    * necessary when this contract's own reattach-dependent tests turned out to be unconditionally
    * required, making the contract, as originally written, unsatisfiable by an honest Tier 3
    * implementation. */
  def reattachSupported: Boolean = true

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

  test("reattach on a fresh handle returns statistics consistent with what was registered, OR is a documented UnsupportedOperationException for a declared Tier 3 backend") {
    val store = newStore()
    val h = freshHandle(store)
    if (reattachSupported) {
      val result = store.reattach(h)
      result.numMappers should be >= 1
      result.numPartitions should be >= 1
      result.bytesByPartition.length shouldBe result.numPartitions
      result.mapperAttempts.length shouldBe result.numMappers
    } else {
      info("this implementation declares reattach unsupported (Tier 3 backend gap) -- checking it fails closed, not silently")
      an[UnsupportedOperationException] should be thrownBy store.reattach(h)
    }
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
    if (reattachSupported) {
      val (r1, r2) = (store.reattach(h), store.reattach(restored))
      r1.numMappers shouldBe r2.numMappers
      r1.numPartitions shouldBe r2.numPartitions
      r1.bytesByPartition.toSeq shouldBe r2.bytesByPartition.toSeq
      r1.rowCount shouldBe r2.rowCount
      r1.mapperAttempts.toSeq shouldBe r2.mapperAttempts.toSeq
    } else {
      // Both the original and the round-tripped handle must fail the SAME documented way --
      // deserializeHandle must not have silently produced a handle that behaves differently.
      an[UnsupportedOperationException] should be thrownBy store.reattach(h)
      an[UnsupportedOperationException] should be thrownBy store.reattach(restored)
    }
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

  test("store/readPartition round-trips real bytes losslessly, OR is a documented " +
    "UnsupportedOperationException for a declared Tier 3 backend") {
    // Rides the SAME reattachSupported flag as the `reattach` test above, deliberately: a backend
    // whose Tier 3 gap blocks reading a shuffle's committed locations under a foreign application
    // identity has the identical capability requirement for store/readPartition (see
    // ExchangeStore.store's own doc comment) -- no separate `storeSupported` flag exists, and
    // adding one here would just invite a testkit user to declare TWO gaps for what is actually
    // ONE checked backend limitation.
    val store = newStore()
    val partitions = Array(
      Array[Byte](1, 2, 3),
      Array[Byte](4, 5, 6, 7),
      Array.emptyByteArray // a real, legitimate case: an empty partition, not a missing one
    )
    if (reattachSupported) {
      val handle = store.store(partitions)
      for (i <- partitions.indices) {
        store.readPartition(handle, i).toSeq shouldBe partitions(i).toSeq
      }
      an[Exception] should be thrownBy store.readPartition(handle, partitions.length) // out of range
    } else {
      info("this implementation declares store/readPartition unsupported (Tier 3 backend gap) -- checking it fails closed, not silently")
      an[UnsupportedOperationException] should be thrownBy store.store(partitions)
    }
  }

  test("neither store nor readPartition leaves the caller sharing a buffer with the backend") {
    // ExchangeStore.store / readPartition both specify caller-owned buffers, and getting either
    // wrong is SILENT: no call fails, the stored partition just quietly becomes something else for
    // every later reader. That makes it exactly the kind of rule that has to live in the contract
    // rather than in one implementation's comment -- a backend returning its own internal buffer
    // (or retaining the caller's) passes every other test in this suite unchanged.
    val store = newStore()
    val original = Array[Byte](1, 2, 3)
    val partitions = Array(original, Array[Byte](4, 5, 6, 7))
    if (!reattachSupported) {
      info("this implementation declares store/readPartition unsupported (Tier 3 backend gap) -- skipped")
    } else {
      val handle = store.store(partitions)

      // WRITE side: the caller still owns what it passed in, and reusing it as a scratch buffer is
      // a real producer pattern, not an abuse.
      original(0) = 99
      store.readPartition(handle, 0).toSeq shouldBe Seq[Byte](1, 2, 3)

      // READ side: what comes back is the caller's to decode/decompress in place.
      val read = store.readPartition(handle, 0)
      read(0) = 42
      store.readPartition(handle, 0).toSeq shouldBe Seq[Byte](1, 2, 3)
    }
  }
}
