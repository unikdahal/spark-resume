package org.apache.spark.resume.fs

import java.nio.file.Files
import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Behavior specific to `FsExchangeStore` that `ExchangeStoreContract` (shared across every
  * `ExchangeStore` implementation) has no vocabulary for -- see `FsExchangeStoreSpec` for the
  * shared conformance suite itself. */
class FsExchangeStoreDirectSpec extends AnyFunSuite with Matchers {

  private val baseDir: String = Files.createTempDirectory("spark-resume-fs-direct-test").toString
  private def newStore(): FsExchangeStore = new FsExchangeStore(baseDir)

  test("reattach on a handle whose manifest was never written throws NoSuchElementException, " +
    "not a silent empty result or the Tier 3 UnsupportedOperationException") {
    // A handle this store never wrote at all (not even a superseded one) -- distinct from
    // ExchangeStoreContract's staleHandle scenario, which is caught by isFresh/SafeReattach
    // BEFORE reattach is ever called. This proves reattach itself fails loudly if that precondition
    // is somehow skipped, rather than returning a nonsensical ReattachResult.
    val store = newStore()
    val neverWritten = FsHandle(s"never-written-${UUID.randomUUID()}", 1L)
    a[NoSuchElementException] should be thrownBy store.reattach(neverWritten)
  }

  test("reattach detects a manifest/data size mismatch as real corruption, not silently trusting the manifest") {
    val slotId = s"corrupt-${UUID.randomUUID()}"
    // Write a slot honestly, THEN truncate its data file on disk directly -- the exact "same-slot
    // truncate-then-rewrite" failure shape ExchangeStore.isFresh's own doc comment warns a naive
    // length-only check can't distinguish; this test checks reattach's own independent defense,
    // not isFresh's.
    val handle = FsExchangeStore.writeSlot(
      baseDir, slotId,
      numMappers = 1, numPartitions = 1,
      bytesByPartition = Array(100L), rowCount = 5L, mapperAttempts = Array(0),
      data = new Array[Byte](100))
    val dataPath = java.nio.file.Paths.get(baseDir, slotId, s"gen-${handle.generation}.data")
    Files.write(dataPath, new Array[Byte](50)) // truncate: manifest still claims 100 bytes

    val store = newStore()
    store.isFresh(handle) shouldBe true // generation marker is untouched -- isFresh alone can't catch this
    an[IllegalStateException] should be thrownBy store.reattach(handle)
  }

  test("writeSlot generations are monotonic per slotId, independent across different slotIds") {
    val slotA = s"slot-a-${UUID.randomUUID()}"
    val slotB = s"slot-b-${UUID.randomUUID()}"
    val a1 = FsExchangeStore.writeSlot(baseDir, slotA, 1, 1, Array(1L), 1L, Array(0), Array[Byte](1))
    val a2 = FsExchangeStore.writeSlot(baseDir, slotA, 1, 1, Array(1L), 1L, Array(0), Array[Byte](1))
    val b1 = FsExchangeStore.writeSlot(baseDir, slotB, 1, 1, Array(1L), 1L, Array(0), Array[Byte](1))
    a1.generation shouldBe 1L
    a2.generation shouldBe 2L
    b1.generation shouldBe 1L // independent counter from slotA's
  }
}
