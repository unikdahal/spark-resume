package org.apache.spark.resume.fs

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths, StandardCopyOption}
import java.util.UUID

import org.apache.spark.resume.api._

/** A second, independent `ExchangeStore` implementation (docs/DESIGN.md §15's planned
  * conformance-suite deliverable) -- deliberately NOT built to prove out a real disaggregated
  * shuffle service (that is what `spark-resume-celeborn` is for). Its entire purpose is to answer
  * a question `spark-resume-celeborn` alone cannot: is `ExchangeStoreContract` actually
  * satisfiable, completely, by someone who did not write it, using only the published testkit?
  * Every method here is real -- real files, real bytes read back off disk, no fixture-only
  * shortcuts -- and needs zero external infrastructure (no running service, no network), which
  * also makes it the easiest first read for anyone considering implementing the SPI themselves.
  *
  * Unlike `CelebornExchangeStore`, `reattach`/`store`/`readPartition` here are NOT documented
  * gaps: this is the first REAL exerciser of `ExchangeStoreContract`'s success paths for all
  * three, and (as of Phase 4) this repo's real proof target for actual execution-skipping
  * (`spark-resume-spark-3.5`'s `AqeExecutionSkipSpikeSpec` and its successors) -- `store`/
  * `readPartition` read and write ACTUAL partition bytes, per-partition-addressable, not a single
  * opaque blob.
  *
  * == On-disk layout, under `baseDir` ==
  * {{{
  * baseDir/<slotId>/current                    one line: the CURRENT generation (Long), written
  *                                              via an atomic rename (see writeGeneration) -- the
  *                                              same "real CAS primitive, not a read-then-write
  *                                              race" property this project's other stores get
  *                                              from Redis INCR / a Lua script / Celeborn's own
  *                                              master, done here with
  *                                              `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)`.
  * baseDir/<slotId>/gen-<N>.manifest           one field per line: numMappers, numPartitions,
  *                                              bytesByPartition (comma-joined), rowCount,
  *                                              mapperAttempts (comma-joined).
  * baseDir/<slotId>/gen-<N>.part-<i>.data      the actual bytes for partition `i` of generation
  *                                              `N` -- per-partition-addressable, so a caller can
  *                                              read ONE partition without touching the others
  *                                              (what a real execution-skip RDD's `compute(i)`
  *                                              needs: reading partition `i` alone, on the
  *                                              executor that will consume it, not the whole
  *                                              stage's output up front on the driver).
  * baseDir/<slotId>/gen-<N>.claim              an empty marker proving generation `N` is taken --
  *                                              the atomic claim primitive itself, see
  *                                              `FsExchangeStore.claimGeneration`. Deliberately
  *                                              never deleted by the write path;
  *                                              `cleanupSupersededGenerations` reclaims these,
  *                                              and the superseded manifests/data alongside them,
  *                                              on a slot no writer is active on.
  * }}}
  *
  * @param baseDir the root directory this store instance operates under -- analogous to
  *   `CelebornExchangeStore`'s `masterRestBaseUrl`: a deployment-level config, not part of any
  *   handle. */
final class FsExchangeStore(baseDir: String) extends ExchangeStore {

  private val root: Path = Paths.get(baseDir)

  override def handleKind: String = "fs"

  override def serializeHandle(handle: ExchangeHandle): Array[Byte] = handle match {
    case h: FsHandle => FsHandleCodec.encode(h)
    case other => throw new IllegalArgumentException(s"not an FsHandle: $other")
  }

  override def deserializeHandle(payload: Array[Byte]): ExchangeHandle = FsHandleCodec.decode(payload)

  override def isFresh(handle: ExchangeHandle): Boolean = handle match {
    case FsHandle(slotId, generation) => currentGeneration(slotId).contains(generation)
    case other => throw new IllegalArgumentException(s"not an FsHandle: $other")
  }

  /** No identity-reuse hazard for a plain filesystem path: unlike Celeborn, there is no
    * backend-level "application identity" a resuming process could collide with by reusing --
    * every read is a plain file read, scoped by `slotId` alone, with no separate registering-
    * identity concept at all. Declared `IsolationOk` deliberately, not by omission -- see
    * `ExchangeStore.checkIdentityIsolation`'s own doc comment on why a `None`-hazard backend must
    * say so explicitly rather than via an empty/no-op implementation that looks the same as one
    * that forgot to check anything. */
  override def checkIdentityIsolation(handle: ExchangeHandle): IsolationResult = handle match {
    case _: FsHandle => IsolationOk
    case other => throw new IllegalArgumentException(s"not an FsHandle: $other")
  }

  override def reattach(handle: ExchangeHandle): ReattachResult = handle match {
    case FsHandle(slotId, generation) =>
      val manifestPath = genPath(slotId, generation, "manifest")
      if (!Files.exists(manifestPath)) {
        // A REAL bug/race, not a documented backend gap: callers are expected to have already
        // confirmed isFresh via SafeReattach before ever reaching here (this store's precondition,
        // same as every other ExchangeStore in this repo) -- reaching this with a stale/missing
        // generation means that contract was violated, and this store does not silently paper
        // over it, matching SafeReattach.attempt's own "any OTHER exception propagates uncaught"
        // posture for anything that isn't the documented Tier 3 signal.
        throw new NoSuchElementException(s"FsExchangeStore.reattach: no manifest for slotId=$slotId generation=$generation")
      }
      val fields = Files.readAllLines(manifestPath, UTF_8)
      val numMappers = fields.get(0).toInt
      val numPartitions = fields.get(1).toInt
      val bytesByPartition = parseLongCsv(fields.get(2))
      val rowCount = fields.get(3).toLong
      val mapperAttempts = parseIntCsv(fields.get(4))
      // Not just trusting the manifest's own numbers: confirms EVERY partition's actual on-disk
      // size is consistent with what the manifest claims for THAT partition -- stronger than the
      // pre-per-partition-storage version of this check, which could only verify the aggregate
      // sum, not which partition (if any) was the one that got truncated.
      for (i <- 0 until numPartitions) {
        val actualSize = Files.size(partPath(slotId, generation, i))
        if (actualSize != bytesByPartition(i)) {
          throw new IllegalStateException(
            s"FsExchangeStore.reattach: manifest claims partition $i is ${bytesByPartition(i)} bytes " +
              s"but gen-$generation.part-$i.data is $actualSize bytes -- corrupt slot")
        }
      }
      ReattachResult(numMappers, numPartitions, bytesByPartition, rowCount, mapperAttempts)
    case other => throw new IllegalArgumentException(s"not an FsHandle: $other")
  }

  override def store(partitions: Array[Array[Byte]]): ExchangeHandle = {
    val slotId = s"store-${UUID.randomUUID()}"
    // A synthesized ReattachResult, not real producer-supplied statistics -- documented
    // simplification, same as InMemoryExchangeStore.store's: a caller wanting REAL per-mapper
    // statistics should write via FsExchangeStore.writeSlot directly (or a future richer `store`
    // overload), not this trait-level method, which by design only knows opaque bytes.
    FsExchangeStore.writeGeneration(
      baseDir, slotId,
      numMappers = 1, mapperAttempts = Array(0), rowCount = 0L,
      dataByPartition = partitions)
  }

  override def readPartition(handle: ExchangeHandle, partitionId: Int): Array[Byte] = handle match {
    case FsHandle(slotId, generation) =>
      val manifestPath = genPath(slotId, generation, "manifest")
      if (!Files.exists(manifestPath)) {
        throw new NoSuchElementException(s"FsExchangeStore.readPartition: no manifest for slotId=$slotId generation=$generation")
      }
      val numPartitions = Files.readAllLines(manifestPath, UTF_8).get(1).toInt
      if (partitionId < 0 || partitionId >= numPartitions) {
        throw new IndexOutOfBoundsException(s"partitionId=$partitionId out of range [0, $numPartitions)")
      }
      val p = partPath(slotId, generation, partitionId)
      if (!Files.exists(p)) {
        throw new NoSuchElementException(s"FsExchangeStore.readPartition: missing $p")
      }
      Files.readAllBytes(p)
    case other => throw new IllegalArgumentException(s"not an FsHandle: $other")
  }

  private def currentGeneration(slotId: String): Option[Long] = {
    val p = slotPath(slotId).resolve("current")
    if (!Files.exists(p)) None
    else Some(Files.readString(p, UTF_8).trim.toLong)
  }

  private def slotPath(slotId: String): Path = root.resolve(slotId)
  private def genPath(slotId: String, generation: Long, suffix: String): Path =
    slotPath(slotId).resolve(s"gen-$generation.$suffix")
  private def partPath(slotId: String, generation: Long, partitionId: Int): Path =
    slotPath(slotId).resolve(s"gen-$generation.part-$partitionId.data")

  private def parseLongCsv(s: String): Array[Long] = if (s.isEmpty) Array.empty else s.split(",").map(_.toLong)
  private def parseIntCsv(s: String): Array[Int] = if (s.isEmpty) Array.empty else s.split(",").map(_.toInt)
}

object FsExchangeStore {

  /** The producer-side write path used directly by test fixtures wanting REAL, caller-supplied
    * statistics (numMappers/mapperAttempts/rowCount) rather than `ExchangeStore.store`'s
    * synthesized ones -- deliberately NOT a method on `ExchangeStore` itself: that trait models
    * only the resuming driver's operations plus the now-added, backend-agnostic `store`/
    * `readPartition` (opaque bytes only, no statistics). Real producers wanting richer statistics
    * call this directly, the same way `CelebornExchangeStoreSpec`'s fixtures drive Celeborn's real
    * data-plane client directly rather than through `ExchangeStore` itself.
    *
    * Claims its generation atomically (`Files.createFile` on a per-generation claim marker, so
    * concurrent writers on one slot always get DISTINCT generations and never write the same data
    * file) and publishes it via write-to-temp-then-atomic-rename -- see [[writeGeneration]]'s
    * `claimGeneration` for exactly what that does and does not guarantee. Note the caller's
    * `numPartitions`/`bytesByPartition` must agree with `data`: they describe how to split it, and
    * a mismatch is rejected rather than silently normalized (an earlier version sliced with
    * `Array.slice`, which CLAMPS, so a declared-but-absent trailing partition became a shorter one
    * and the manifest recorded the shorter size -- a fixture trying to build an inconsistent slot
    * silently got a perfectly consistent one instead). */
  def writeSlot(
      baseDir: String,
      slotId: String,
      numMappers: Int,
      numPartitions: Int,
      bytesByPartition: Array[Long],
      rowCount: Long,
      mapperAttempts: Array[Int],
      data: Array[Byte]): FsHandle = {
    // Legacy single-blob callers: split `data` back into per-partition slices matching
    // `bytesByPartition`'s claimed sizes, so the underlying storage format stays UNIFIED
    // (always per-partition files) rather than maintaining two on-disk shapes.
    require(bytesByPartition.length == numPartitions,
      s"FsExchangeStore.writeSlot: numPartitions=$numPartitions but bytesByPartition has " +
        s"${bytesByPartition.length} entries")
    require(bytesByPartition.forall(_ >= 0L),
      s"FsExchangeStore.writeSlot: negative partition size in ${bytesByPartition.mkString(",")}")
    require(bytesByPartition.sum == data.length.toLong,
      s"FsExchangeStore.writeSlot: bytesByPartition sums to ${bytesByPartition.sum} but data is " +
        s"${data.length} bytes -- these must agree exactly; silently clamping would write a slot " +
        "whose manifest disagrees with what the caller asked for")
    // The symmetric guard for the OTHER pair in this manifest, and it matters for the same reason
    // the three above do: `ReattachResult`'s own doc comment records that a backend whose read path
    // filters by attempt id would silently drop a mapper's real data under an all-zero placeholder,
    // so a slot claiming N mappers with a different number of attempt ids is exactly the kind of
    // silently-inconsistent fixture these preconditions exist to reject rather than write.
    require(mapperAttempts.length == numMappers,
      s"FsExchangeStore.writeSlot: numMappers=$numMappers but mapperAttempts has " +
        s"${mapperAttempts.length} entries -- one commit attempt id per mapper, no placeholders")
    val dataByPartition = new Array[Array[Byte]](numPartitions)
    var offset = 0
    for (i <- 0 until numPartitions) {
      val len = bytesByPartition(i).toInt
      dataByPartition(i) = data.slice(offset, offset + len)
      offset += len
    }
    writeGeneration(baseDir, slotId, numMappers, mapperAttempts, rowCount, dataByPartition)
  }

  /** The real per-partition write path -- used by both [[FsExchangeStore.store]] (synthesized
    * stats) and [[writeSlot]] (caller-supplied stats, for test fixtures wanting a real
    * `numMappers`/`rowCount` to assert against). */
  private[fs] def writeGeneration(
      baseDir: String,
      slotId: String,
      numMappers: Int,
      mapperAttempts: Array[Int],
      rowCount: Long,
      dataByPartition: Array[Array[Byte]]): FsHandle = {
    val root = Paths.get(baseDir)
    val slotDir = root.resolve(slotId)
    Files.createDirectories(slotDir)

    val currentPath = slotDir.resolve("current")
    val nextGen = claimGeneration(slotDir, currentPath)

    val numPartitions = dataByPartition.length
    val bytesByPartition = dataByPartition.map(_.length.toLong)
    dataByPartition.zipWithIndex.foreach { case (bytes, i) =>
      Files.write(slotDir.resolve(s"gen-$nextGen.part-$i.data"), bytes)
    }

    val manifestPath = slotDir.resolve(s"gen-$nextGen.manifest")
    val manifestText = Seq(
      numMappers.toString,
      numPartitions.toString,
      bytesByPartition.mkString(","),
      rowCount.toString,
      mapperAttempts.mkString(",")).mkString("\n")
    Files.write(manifestPath, manifestText.getBytes(UTF_8))

    // Per-writer temp name, NOT a shared `current.tmp`: two writers on the same slot would
    // otherwise race on one path, and whichever moved second would fail outright with
    // NoSuchFileException because the first had already consumed it.
    val tmpCurrent = slotDir.resolve(s"current.tmp.$nextGen")
    Files.write(tmpCurrent, nextGen.toString.getBytes(UTF_8))
    Files.move(tmpCurrent, currentPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)

    FsHandle(slotId, nextGen)
  }

  /** Deletes every artifact of a SUPERSEDED generation in `slotId` -- the `gen-N.claim`,
    * `gen-N.manifest` and `gen-N.part-i.data` files of every generation below the one `current`
    * points at, plus any `current.tmp.N` a writer died between writing and renaming. Returns how
    * many files were deleted. A slot with no `current` pointer, or no directory at all, is left
    * alone and reports `0`.
    *
    * Exists because nothing else ever removes those files: [[writeGeneration]] adds one claim
    * marker (and one manifest, and one data file per partition) per write and never revisits them,
    * so a slot rewritten repeatedly grows without bound, and `claimGeneration` below has to scan
    * past every claim ever made on it. This is the entry point that makes the growth reclaimable
    * instead of permanent -- deliberately an explicit maintenance call rather than something
    * `writeGeneration` does implicitly, for the reason in the next paragraph.
    *
    * ==Only safe on a QUIESCENT slot==
    * Must not run while any writer is working on `slotId`. `current` is last-writer-wins across
    * concurrent writers (see [[claimGeneration]]), so a generation BELOW `current` can still be one
    * a slower writer is actively writing -- deleting its claim marker would let a third writer
    * claim that same number and interleave two datasets into one generation, precisely the
    * corruption the claim marker exists to prevent. Readers are fine: every generation this deletes
    * is one `isFresh` already reports `false` for, and callers are required to check that before
    * reading (see [[FsExchangeStore.reattach]]). */
  def cleanupSupersededGenerations(baseDir: String, slotId: String): Int = {
    val slotDir = Paths.get(baseDir).resolve(slotId)
    val currentPath = slotDir.resolve("current")
    if (!Files.isDirectory(slotDir) || !Files.exists(currentPath)) {
      0
    } else {
      val current = Files.readString(currentPath, UTF_8).trim.toLong
      // Collected first, deleted after: mutating a directory while its own DirectoryStream is open
      // is not something java.nio guarantees anything about.
      val doomed = scala.collection.mutable.ArrayBuffer.empty[Path]
      val stream = Files.newDirectoryStream(slotDir)
      try {
        val it = stream.iterator()
        while (it.hasNext) {
          val p = it.next()
          if (isSuperseded(p.getFileName.toString, current)) doomed += p
        }
      } finally {
        stream.close()
      }
      doomed.count(Files.deleteIfExists)
    }
  }

  /** Whether `fileName` belongs to a generation `current` has already superseded. `current.tmp.N`
    * is included for `N <= current` only: a HIGHER one may belong to a writer that has claimed its
    * generation and not yet published it. */
  private def isSuperseded(fileName: String, current: Long): Boolean = {
    if (fileName.startsWith("gen-")) {
      generationIn(fileName.stripPrefix("gen-")).exists(_ < current)
    } else if (fileName.startsWith("current.tmp.")) {
      generationIn(fileName.stripPrefix("current.tmp.")).exists(_ <= current)
    } else {
      false // `current` itself, and anything a future version of this store adds
    }
  }

  /** The leading generation number in `rest` (everything up to the first `.`), or `None` if it is
    * not a number at all -- an unrecognized file is left alone rather than guessed at. */
  private def generationIn(rest: String): Option[Long] = {
    val digits = rest.takeWhile(_ != '.')
    try Some(digits.toLong) catch { case _: NumberFormatException => None }
  }

  /** The highest generation any claim marker in `slotDir` has ever taken, or `0` if there are
    * none. Read to START [[claimGeneration]]'s search above every existing claim instead of at
    * `current + 1`: `current` can legitimately point BELOW the highest claim (the lost-update case
    * documented in `claimGeneration`), and walking up from there one `createFile` at a time made
    * every write on a long-lived slot O(generations) in syscalls and, past
    * `MaxGenerationClaimAttempts`, made the slot permanently unwritable. */
  private def highestClaimedGeneration(slotDir: Path): Long = {
    val stream = Files.newDirectoryStream(slotDir, "gen-*.claim")
    try {
      var highest = 0L
      val it = stream.iterator()
      while (it.hasNext) {
        val n = generationIn(it.next().getFileName.toString.stripPrefix("gen-")).getOrElse(0L)
        if (n > highest) highest = n
      }
      highest
    } finally {
      stream.close()
    }
  }

  /** Claims a generation number that no other writer on this slot can also be using, by creating
    * `gen-N.claim` with `Files.createFile` -- which is atomic (`O_CREAT | O_EXCL` underneath) and
    * throws `FileAlreadyExistsException` rather than succeeding if someone else got there first.
    * The loser simply advances and retries, so every concurrent writer ends up with a DISTINCT N.
    * The search starts above BOTH `current` and the highest existing claim (see
    * [[highestClaimedGeneration]]), so the retry loop only ever runs against genuinely concurrent
    * writers rather than against the accumulated history of the slot.
    *
    * This is what makes the generation bump a real claim rather than the read-then-write it used
    * to be: reading `current`, adding one, and only publishing ~20 lines later left a window in
    * which two writers both computed the same N and then both wrote the SAME `gen-N.part-i.data`
    * paths, interleaving two datasets into one generation that `isFresh` would then report as
    * fresh. Distinct N per writer removes that entirely -- no two writers ever address the same
    * data file, so a published generation is always exactly one writer's own, self-consistent
    * output.
    *
    * What this deliberately does NOT claim: the final `current` pointer update is last-writer-wins
    * across concurrent writers, so a slower writer holding a lower N can still publish after a
    * faster one with a higher N, leaving `current` pointing at the older (complete, uncorrupted)
    * generation. That is a lost UPDATE, not corruption -- every reader still sees some whole,
    * internally consistent generation, which is the property `reattach`/`readPartition` actually
    * depend on. A backend needing strict monotonic publication needs a real compare-and-set on the
    * pointer, which a POSIX filesystem does not offer; `spark-resume-redis` gets that from Redis
    * itself, and this store does not pretend to.
    *
    * Claim markers are never removed by this method (removing one would reopen the race it closed).
    * [[cleanupSupersededGenerations]] is the entry point that reclaims them, on a quiescent slot. */
  private def claimGeneration(slotDir: Path, currentPath: Path): Long = {
    val priorGen = if (Files.exists(currentPath)) Files.readString(currentPath, UTF_8).trim.toLong else 0L
    var candidate = math.max(priorGen, highestClaimedGeneration(slotDir)) + 1
    var attempts = 0
    while (attempts < MaxGenerationClaimAttempts) {
      try {
        Files.createFile(slotDir.resolve(s"gen-$candidate.claim"))
        return candidate
      } catch {
        case _: FileAlreadyExistsException =>
          candidate += 1
          attempts += 1
      }
    }
    throw new IllegalStateException(
      s"FsExchangeStore: could not claim a free generation for slot ${slotDir.getFileName} after " +
        s"$MaxGenerationClaimAttempts attempts (last tried $candidate) -- concurrent writers on " +
        "one slot far beyond anything this store is designed for. Since the search starts above " +
        "the highest existing claim, accumulated history alone cannot cause this; " +
        "cleanupSupersededGenerations reclaims that history on a quiescent slot.")
  }

  private val MaxGenerationClaimAttempts = 1000
}
