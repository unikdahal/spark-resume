package org.apache.spark.resume.api

/** An in-memory, backend-owned handle to one completed stage's shuffle output. A concrete
  * `ExchangeStore` implementation defines its own subtype and is the only code that ever
  * constructs or interprets one -- this project never inspects a handle's internals.
  *
  * Deliberately NOT what gets persisted in an [[Anchor]]. A live handle can hold backend-internal
  * objects (open connections, non-Java-serializable wire types) that must never leak into the
  * anchor-store's persistence format, and an `AnchorStore` implementation must be writable
  * without depending on any `ExchangeStore` implementation's classes. See
  * [[ExchangeStore.serializeHandle]] / [[ExchangeStore.deserializeHandle]] for the boundary that
  * enforces this split, and [[Anchor.handlePayload]]'s doc comment for the failure this avoids.
  *
  * Extends `java.io.Serializable` (added in Phase 4, alongside [[ExchangeStore.store]]/
  * [[ExchangeStore.readPartition]]): a real execution-skip RDD needs to ship a live handle to
  * EXECUTORS (each partition's `compute()` reads its own bytes independently, not funneled
  * through the driver), which requires actual Java serializability, not just an in-process
  * reference -- see `spark-resume-spark-3.5`'s `SkippedShuffleRDD`. Every concrete handle in this
  * repo (`FsHandle`, `CelebornHandle`, `InMemoryHandle`) is already a plain case class over
  * serializable fields, so this costs existing implementations nothing. */
trait ExchangeHandle extends Serializable
