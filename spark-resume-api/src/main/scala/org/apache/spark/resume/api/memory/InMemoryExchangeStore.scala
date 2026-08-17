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
    var identityConflict: Boolean)

/** A reference [[ExchangeStore]] implementation backed by an in-process map. NOT a production
  * store -- it has no persistence and no cross-process visibility, which makes it useless for
  * the one thing this whole project exists for (surviving a driver PROCESS restart). It exists
  * for two purposes: (1) a zero-infrastructure way to try the admission engine locally, and (2)
  * the first, reference implementation the testkit contracts in
  * `org.apache.spark.resume.api.testkit` are proven against -- see
  * `org.apache.spark.resume.api.testkit.ExchangeStoreContract`. */
final class InMemoryExchangeStore extends ExchangeStore {
  private val records = new ConcurrentHashMap[String, Record]()

  override def handleKind: String = "in-memory"

  override def serializeHandle(handle: ExchangeHandle): Array[Byte] = handle match {
    case InMemoryHandle(id) => id.getBytes("UTF-8")
    case other => throw new IllegalArgumentException(s"not an InMemoryHandle: $other")
  }

  override def deserializeHandle(payload: Array[Byte]): ExchangeHandle =
    InMemoryHandle(new String(payload, "UTF-8"))

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
}
