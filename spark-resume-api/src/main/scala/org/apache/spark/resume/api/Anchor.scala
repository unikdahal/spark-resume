package org.apache.spark.resume.api

/** The durable record one completed stage leaves behind, as persisted by an [[AnchorStore]].
  *
  * @param handlePayload the backend's OWN serialized form of its [[ExchangeHandle]], produced by
  *   `ExchangeStore.serializeHandle` and turned back into a live handle by
  *   `ExchangeStore.deserializeHandle` -- never a live handle object itself. This is deliberate,
  *   not a convenience: a live handle can carry backend-internal state (open connections, wire
  *   types that don't round-trip Java serialization) that an `AnchorStore` implementation must
  *   never need to know how to serialize. Typing this field as `ExchangeHandle` directly would
  *   make every `AnchorStore` implementation transitively depend on every `ExchangeStore`
  *   implementation whose handles it might ever be asked to store -- exactly the coupling the
  *   split between the two store interfaces exists to avoid. `handleKind` disambiguates which
  *   `ExchangeStore` implementation's payload format this is, so a store or tool inspecting
  *   anchors from multiple backends doesn't have to guess.
  * @param generation the [[AnchorStore.acquireGeneration]] fence this anchor was written under.
  */
final case class Anchor(
    schemaVersion: String,
    queryId: String,
    generation: Long,
    fingerprint: String,
    handleKind: String,
    handlePayload: Array[Byte],
    numMappers: Int,
    numPartitions: Int,
    bytesByPartition: Array[Long],
    rowCount: Long,
    createdAtMs: Long)
