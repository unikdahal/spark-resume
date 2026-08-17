package org.apache.spark.resume.fs

import java.nio.file.Files
import java.util.UUID

import org.apache.spark.resume.api.{ExchangeHandle, ExchangeStore}
import org.apache.spark.resume.api.testkit.ExchangeStoreContract

/** Runs the full `ExchangeStoreContract` conformance suite against a REAL `FsExchangeStore` --
  * real files on a real temp directory, not a mock of either the store or the testkit. See
  * `FsExchangeStore`'s doc comment for why this module exists: it is the second, independent
  * `ExchangeStore` implementation in this repo, and the first to exercise the contract's
  * reattach-SUCCESS path with `reattachSupported` left at its default `true` -- every other real
  * store in this repo (`spark-resume-celeborn`) declares it `false`, so until this module, that
  * path had only ever been run against `InMemoryExchangeStore`, the reference implementation the
  * contract was originally written against.
  *
  * `staleHandle` here is deliberately a REAL supersession (write generation 1, then write AGAIN
  * to bump the same slot to generation 2, hand back the handle for generation 1) rather than
  * "never registered" (`spark-resume-celeborn`'s choice, a real, disclosed, narrower scoping
  * decision there) -- this backend makes an actual generation-bump trivial to drive directly, so
  * there is no reason not to test the stronger claim. */
class FsExchangeStoreSpec extends ExchangeStoreContract {

  private val baseDir: String = Files.createTempDirectory("spark-resume-fs-test").toString

  override def newStore(): ExchangeStore = new FsExchangeStore(baseDir)

  override def freshHandle(store: ExchangeStore): ExchangeHandle = {
    val slotId = s"fresh-${UUID.randomUUID()}"
    FsExchangeStore.writeSlot(
      baseDir, slotId,
      numMappers = 2, numPartitions = 3,
      bytesByPartition = Array(10L, 20L, 30L),
      rowCount = 100L,
      mapperAttempts = Array(0, 0),
      data = new Array[Byte](60)) // 10 + 20 + 30, matching bytesByPartition's claimed sum
  }

  override def staleHandle(store: ExchangeStore): ExchangeHandle = {
    val slotId = s"stale-${UUID.randomUUID()}"
    val gen1 = FsExchangeStore.writeSlot(
      baseDir, slotId,
      numMappers = 1, numPartitions = 1,
      bytesByPartition = Array(5L), rowCount = 10L, mapperAttempts = Array(0),
      data = new Array[Byte](5))
    // A real supersession, not a synthetic "never existed" handle: bump the SAME slot to
    // generation 2, then hand back generation 1's now-superseded handle.
    FsExchangeStore.writeSlot(
      baseDir, slotId,
      numMappers = 1, numPartitions = 1,
      bytesByPartition = Array(7L), rowCount = 20L, mapperAttempts = Array(0),
      data = new Array[Byte](7))
    gen1
  }

  override def conflictingIdentityHandle(store: ExchangeStore): Option[ExchangeHandle] = None
}
