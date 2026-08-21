package org.apache.spark.resume.api.memory

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.resume.api._

/** A single-JVM handle -- carries nothing but an id, because everything the store needs to know
  * about that id lives in the store's own map, not on the handle. Real backends carry whatever
  * addressing their protocol needs instead; this shape is specific to being a dev/test double. */
final case class InMemoryHandle(id: String) extends ExchangeHandle

private final case class Record(
    result: ReattachResult,
    var superseded: Boolean,
    var identityConflict: Boolean,
    partitions: Option[Array[Array[Byte]]] = None)

/** A reference [[ExchangeStore]] implementation backed by an in-process map. NOT a production
  * store -- it has no persistence and no cross-process visibility, which makes it useless for
  * the one thing this whole project exists for (surviving a driver PROCESS restart). It exists
  * for two purposes: (1) a zero-infrastructure way to try the admission engine locally, and (2)
  * the first, reference implementation the testkit contracts in
  * `org.apache.spark.resume.api.testkit` are proven against -- see
  * `org.apache.spark.resume.api.testkit.ExchangeStoreContract`.
  *
  * Extends `java.io.Serializable` (Phase 4) -- NOT true of `ExchangeStore` implementations in
  * general (a real backend's store is meant to be reconstructed per-executor from a small config
  * closure, e.g. `spark-resume-fs`'s `() => new FsExchangeStore(baseDir)`, never shipped as a
  * live instance -- see `ExecutionSkipRule`'s `storeFactory` doc comment). This class is the one
  * legitimate exception: its whole state IS a plain `ConcurrentHashMap` over serializable content
  * (no open connections, no backend-internal wire state), so Java-serializing it is
  * content-preserving, not a workaround -- letting a same-JVM test (`local[]` mode still
  * round-trips task closures through real serialization, by design, for parity with cluster mode)
  * exercise a real execution-skip RDD without needing an actual multi-process backend. */
final class InMemoryExchangeStore extends ExchangeStore with Serializable {
  private val records = new ConcurrentHashMap[String, Record]()

  override def handleKind: String = "in-memory"

  // A fixed magic prefix, not just the raw id -- so deserializeHandle can actually tell a foreign
  // payload apart from one of its own, per ExchangeStore.deserializeHandle's contract ("must not
  // silently accept a foreign payload"). Bare bytes with no tag would make every payload look
  // like a valid id, which is exactly the silent-acceptance failure that contract forbids.
  private val Magic = "IMH1:"

  override def serializeHandle(handle: ExchangeHandle): Array[Byte] = handle match {
    case InMemoryHandle(id) => (Magic + id).getBytes("UTF-8")
    case other => throw new IllegalArgumentException(s"not an InMemoryHandle: $other")
  }

  override def deserializeHandle(payload: Array[Byte]): ExchangeHandle = {
    val text = new String(payload, "UTF-8")
    if (!text.startsWith(Magic)) {
      throw new IllegalArgumentException(
        s"payload was not produced by InMemoryExchangeStore (missing '$Magic' tag)")
    }
    InMemoryHandle(text.stripPrefix(Magic))
  }

  /** Test/dev-only registration -- a real backend's equivalent is whatever committed the data in
    * the first place, not a method on the store interface itself (which is why this isn't part
    * of the `ExchangeStore` trait). */
  def put(id: String, result: ReattachResult): InMemoryHandle = {
    records.put(id, Record(result, superseded = false, identityConflict = false))
    InMemoryHandle(id)
  }

  /** Test-only: simulate the backend having superseded this handle's data (a same-slot rewrite,
    * or a genuine deletion) -- exercises `isFresh`'s "no longer valid" path. */
  def markSuperseded(handle: InMemoryHandle): Unit =
    Option(records.get(handle.id)).foreach(_.superseded = true)

  /** Test-only: simulate the identity-reuse hazard `checkIdentityIsolation` exists to catch (see
    * that method's doc comment on [[ExchangeStore]]) -- exercises the conflict path. */
  def markIdentityConflict(handle: InMemoryHandle): Unit =
    Option(records.get(handle.id)).foreach(_.identityConflict = true)

  override def isFresh(handle: ExchangeHandle): Boolean = handle match {
    case h: InMemoryHandle => Option(records.get(h.id)).exists(!_.superseded)
    case _ => false
  }

  override def checkIdentityIsolation(handle: ExchangeHandle): IsolationResult = handle match {
    case h: InMemoryHandle =>
      Option(records.get(h.id)) match {
        case Some(r) if r.identityConflict =>
          IsolationConflict(s"in-memory test conflict marked for handle ${h.id}")
        case Some(_) => IsolationOk
        case None => IsolationConflict(s"unknown handle ${h.id}")
      }
    case other => IsolationConflict(s"not an InMemoryHandle: $other")
  }

  override def reattach(handle: ExchangeHandle): ReattachResult = handle match {
    case h: InMemoryHandle =>
      Option(records.get(h.id)).map(_.result)
        .getOrElse(throw new NoSuchElementException(s"no record for handle ${h.id}"))
    case other => throw new IllegalArgumentException(s"not an InMemoryHandle: $other")
  }

  override def store(partitions: Array[Array[Byte]]): ExchangeHandle = {
    val id = java.util.UUID.randomUUID().toString
    // Defensive copy on the WRITE side, the mirror of readPartition's below and required by the
    // same SPI rule (see ExchangeStore.store): a real backend durably copies the caller's bytes
    // somewhere -- FsExchangeStore writes them to disk and never looks at the caller's arrays
    // again -- so a producer reusing one scratch buffer across partitions is a legitimate caller.
    // Retaining these arrays by reference would let such a producer mutate data already "stored,"
    // silently changing what every later readPartition returns and leaving the bytesByPartition
    // snapshotted just below describing something that no longer exists.
    val owned = partitions.map(_.clone())
    // A synthesized ReattachResult, not real producer-supplied statistics -- this is the
    // reference/dev-only store, whose whole point is being usable with zero setup; a caller
    // needing REAL statistics from a REAL production run should be using a real backend
    // (spark-resume-fs, spark-resume-celeborn), not this one. numMappers=1 / a single all-zero
    // mapperAttempts entry per partition is a documented simplification for this store alone.
    val result = ReattachResult(
      numMappers = 1,
      numPartitions = owned.length,
      bytesByPartition = owned.map(_.length.toLong),
      rowCount = 0L,
      mapperAttempts = Array(0))
    records.put(id, Record(result, superseded = false, identityConflict = false, partitions = Some(owned)))
    InMemoryHandle(id)
  }

  override def readPartition(handle: ExchangeHandle, partitionId: Int): Array[Byte] = handle match {
    case h: InMemoryHandle =>
      val record = Option(records.get(h.id))
        .getOrElse(throw new NoSuchElementException(s"no record for handle ${h.id}"))
      val parts = record.partitions.getOrElse(
        throw new NoSuchElementException(s"handle ${h.id} was registered via put(), not store() -- no partition bytes to read"))
      if (partitionId < 0 || partitionId >= parts.length) {
        throw new IndexOutOfBoundsException(s"partitionId=$partitionId out of range [0, ${parts.length})")
      }
      // Defensive copy, as ExchangeStore.readPartition's contract requires of every implementation
      // (and as FsExchangeStore gets for free from Files.readAllBytes): without it this store alone
      // hands back its own internal buffer, so a caller that decoded or decompressed in place would
      // silently corrupt the stored partition for every later reader. Enforced for all backends by
      // ExchangeStoreContract's buffer-ownership test, not just asserted here.
      parts(partitionId).clone()
    case other => throw new IllegalArgumentException(s"not an InMemoryHandle: $other")
  }
}
