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
    // Write a slot honestly, THEN truncate ONE partition's data file on disk directly -- the exact
    // "same-slot truncate-then-rewrite" failure shape ExchangeStore.isFresh's own doc comment warns
    // a naive length-only check can't distinguish; this test checks reattach's own independent
    // defense, not isFresh's. Two partitions, only one truncated, proves the check is genuinely
    // PER-PARTITION, not just an aggregate sum that a compensating corruption elsewhere could evade.
    val handle = FsExchangeStore.writeSlot(
      baseDir, slotId,
      numMappers = 1, numPartitions = 2,
      bytesByPartition = Array(100L, 40L), rowCount = 5L, mapperAttempts = Array(0),
      data = new Array[Byte](140)) // 100 + 40, split across 2 partition files by writeSlot
    val part0Path = java.nio.file.Paths.get(baseDir, slotId, s"gen-${handle.generation}.part-0.data")
    Files.write(part0Path, new Array[Byte](50)) // truncate partition 0: manifest still claims 100 bytes

    val store = newStore()
    store.isFresh(handle) shouldBe true // generation marker is untouched -- isFresh alone can't catch this
    an[IllegalStateException] should be thrownBy store.reattach(handle)
  }

  test("readPartition reads back the exact bytes for the partition asked, not another one") {
    val store = newStore()
    val partitions = Array(Array[Byte](1, 2, 3), Array[Byte](9, 9), Array.emptyByteArray)
    val handle = store.store(partitions)
    store.readPartition(handle, 0).toSeq shouldBe Seq[Byte](1, 2, 3)
    store.readPartition(handle, 1).toSeq shouldBe Seq[Byte](9, 9)
    store.readPartition(handle, 2).toSeq shouldBe Seq.empty[Byte]
    an[IndexOutOfBoundsException] should be thrownBy store.readPartition(handle, -1)
    an[IndexOutOfBoundsException] should be thrownBy store.readPartition(handle, 3)
  }

  test("writeSlot rejects a mapperAttempts array that disagrees with numMappers") {
    // The symmetric case to the numPartitions/bytesByPartition checks above: a manifest claiming 4
    // mappers with a single attempt id is exactly the silently-inconsistent slot ReattachResult's
    // own doc comment warns about (a backend filtering by attempt id would drop three mappers'
    // real data under an all-zero placeholder), and it used to be accepted and written to disk.
    an[IllegalArgumentException] should be thrownBy FsExchangeStore.writeSlot(
      baseDir, s"bad-attempts-${UUID.randomUUID()}",
      numMappers = 4, numPartitions = 1,
      bytesByPartition = Array(1L), rowCount = 1L, mapperAttempts = Array(0),
      data = Array[Byte](1))
  }

  test("cleanupSupersededGenerations reclaims old generations and leaves the current one readable") {
    val slotId = s"cleanup-${UUID.randomUUID()}"
    val gen1 = FsExchangeStore.writeSlot(baseDir, slotId, 1, 1, Array(1L), 1L, Array(0), Array[Byte](7))
    val gen2 = FsExchangeStore.writeSlot(baseDir, slotId, 1, 1, Array(2L), 1L, Array(0), Array[Byte](8, 9))
    val store = newStore()
    store.isFresh(gen1) shouldBe false
    store.isFresh(gen2) shouldBe true

    val slotDir = java.nio.file.Paths.get(baseDir, slotId)
    Files.exists(slotDir.resolve(s"gen-${gen1.generation}.claim")) shouldBe true // nothing removes these on write

    // Every gen-1 artifact goes: the claim marker, the manifest, and the partition data. Without
    // this entry point they accumulate for the life of the slot, one set per write, with no way to
    // reclaim them short of deleting files by hand.
    FsExchangeStore.cleanupSupersededGenerations(baseDir, slotId) should be >= 3
    Files.exists(slotDir.resolve(s"gen-${gen1.generation}.claim")) shouldBe false
    Files.exists(slotDir.resolve(s"gen-${gen1.generation}.manifest")) shouldBe false
    Files.exists(slotDir.resolve(s"gen-${gen1.generation}.part-0.data")) shouldBe false

    // The CURRENT generation is untouched and still fully readable -- cleanup reclaims history,
    // it does not disturb the data anyone could legitimately still be reading.
    store.isFresh(gen2) shouldBe true
    store.readPartition(gen2, 0).toSeq shouldBe Seq[Byte](8, 9)
    store.reattach(gen2).numPartitions shouldBe 1

    // And the slot is still writable, with a DISTINCT, higher generation -- cleanup must not look
    // like a counter reset to the next writer.
    FsExchangeStore.writeSlot(baseDir, slotId, 1, 1, Array(1L), 1L, Array(0), Array[Byte](1))
      .generation shouldBe 3L
  }

  test("a stale claim marker from a crashed writer does not make later writes rescan it") {
    val slotId = s"stale-claim-${UUID.randomUUID()}"
    FsExchangeStore.writeSlot(baseDir, slotId, 1, 1, Array(1L), 1L, Array(0), Array[Byte](1))
    // A writer that claimed generation 500 and died before publishing: `current` still says 1, so a
    // search starting at `current + 1` would walk up one createFile syscall at a time through every
    // claim in between -- and, once such a run exceeds MaxGenerationClaimAttempts, would leave the
    // slot permanently unwritable. Starting above the HIGHEST claim makes the next write O(1)
    // regardless of how much history the slot has.
    Files.createFile(java.nio.file.Paths.get(baseDir, slotId, "gen-500.claim"))
    FsExchangeStore.writeSlot(baseDir, slotId, 1, 1, Array(1L), 1L, Array(0), Array[Byte](2))
      .generation shouldBe 501L
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
