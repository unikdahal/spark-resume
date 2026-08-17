package org.apache.spark.resume.api.memory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.spark.resume.api.{Anchor, AnchorStore}

/** A reference [[AnchorStore]] implementation backed by in-process concurrent maps. NOT a
  * production store -- no persistence, no cross-process visibility, same caveat as
  * [[InMemoryExchangeStore]]. First implementation the testkit's `AnchorStoreContract` is proven
  * against, including the fencing-under-real-concurrency test (see that contract's doc comment
  * for why sequential-call testing alone would not have been enough). */
final class InMemoryAnchorStore extends AnchorStore {
  private val generations = new ConcurrentHashMap[String, AtomicLong]()
  private val anchors = new ConcurrentHashMap[String, ConcurrentHashMap[String, Anchor]]()

  override def acquireGeneration(queryId: String): Long =
    generations.computeIfAbsent(queryId, _ => new AtomicLong(0L)).incrementAndGet()

  override def putAnchor(generation: Long, anchor: Anchor): Boolean = {
    val current = generations.computeIfAbsent(anchor.queryId, _ => new AtomicLong(0L)).get()
    if (generation != current) {
      false
    } else {
      val perQuery = anchors.computeIfAbsent(anchor.queryId, _ => new ConcurrentHashMap())
      // Keyed by fingerprint: a second write for the same fingerprint in the same generation
      // overwrites, matching the file-backed prototype store's one-file-per-key semantics.
      perQuery.put(anchor.fingerprint, anchor)
      true
    }
  }

  override def loadAnchors(queryId: String): Seq[Anchor] =
    Option(anchors.get(queryId)).map(_.values().asScala.toSeq).getOrElse(Seq.empty)
}
