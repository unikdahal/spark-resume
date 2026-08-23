package org.apache.spark.resume.celeborn

import org.apache.celeborn.rest.v1.master.ShuffleApi
import org.apache.celeborn.rest.v1.master.invoker.ApiClient

import org.apache.spark.resume.api._

/** The first real, cross-process [[ExchangeStore]] implementation (docs/DESIGN.md §14 Phase 3):
  * Apache Celeborn-backed, against a vanilla, published Celeborn release (`0.7.0`) -- not a
  * private fork.
  *
  * [[handleKind]], [[serializeHandle]]/[[deserializeHandle]], [[isFresh]], and
  * [[checkIdentityIsolation]] are real, tested against a real Celeborn master's admin REST API
  * (`celeborn-openapi-client`'s `ShuffleApi`) -- not the shuffle data-plane client
  * (`celeborn-client-spark-3-shaded`) a real Spark application already has on its own classpath
  * for its actual shuffle manager. This store never touches shuffle bytes; it only answers
  * metadata questions the master itself already tracks.
  *
  * [[reattach]] is NOT implemented, and says so loudly rather than faking it. This is docs/DESIGN.md
  * §8's Tier 3 exactly as specified there: "a documented backend-patch requirement... a property
  * of the shuffle service, not of Spark." Verified by checking vanilla Celeborn `0.7.0`'s actual
  * public client API (`ShuffleClient`/`LifecycleManager`), not assumed: `ShuffleClient.readPartition`
  * and `registerMapPartitionTask` are scoped to the `LifecycleManager`/application that registered
  * the shuffle -- there is no public method anywhere in the client API to read a shuffle's
  * committed partition locations under a DIFFERENT application's identity. Adding that capability
  * would require patching `LifecycleManager` itself (non-upstream additions), which this project's
  * clean-room, vanilla-Celeborn-only posture rules out. `reattach` throws
  * `UnsupportedOperationException` naming this gap explicitly; `SafeReattach.attempt`
  * (spark-resume-core) converts that into the structured `RefusedUnsupported` outcome, so a caller
  * doing everything right gets a refusal, not a raw throw. [[store]]/[[readPartition]] (added
  * later than the rest of this class -- see `ExchangeStore.store`'s own doc comment) throw the
  * same way, for the same reason: this store never touches shuffle bytes at all, read or write.
  *
  * @param masterRestBaseUrl e.g. `"http://localhost:9098"` -- the Celeborn master's admin REST
  *   endpoint (NOT its RPC port; a separate port, configured via `celeborn.master.http.host`/
  *   `celeborn.master.http.port` on the master).
  * @param callerAppUniqueId this DRIVER PROCESS's own Celeborn application identity -- the value
  *   this process would register shuffles under, if it were producing rather than resuming. Used
  *   by [[checkIdentityIsolation]] to detect the unfenced same-namespace hazard. A different ID
  *   avoids collision but cannot address the producer's keys; a patched backend must instead use
  *   the stable namespace with a newer driver epoch (see that method's doc comment). */
final class CelebornExchangeStore(masterRestBaseUrl: String, callerAppUniqueId: String) extends ExchangeStore {

  private val shuffleApi: ShuffleApi = {
    val apiClient = new ApiClient()
    apiClient.setBasePath(masterRestBaseUrl)
    new ShuffleApi(apiClient)
  }

  override def handleKind: String = "celeborn"

  override def serializeHandle(handle: ExchangeHandle): Array[Byte] = handle match {
    case h: CelebornHandle => CelebornHandleCodec.encode(h)
    case other => throw new IllegalArgumentException(s"not a CelebornHandle: $other")
  }

  override def deserializeHandle(payload: Array[Byte]): ExchangeHandle =
    CelebornHandleCodec.decode(payload)

  /** Queries the master's admin REST API for the set of currently-registered shuffle ids and
    * checks whether `handle`'s is among them. This is a real, live query against the master's own
    * bookkeeping -- not a local cache or an assumption -- so it correctly reports `false` once the
    * shuffle has been unregistered/expired at the master, exactly the "supersession" signal A-5
    * (freshness is backend-authoritative) requires. */
  override def isFresh(handle: ExchangeHandle): Boolean = handle match {
    case h: CelebornHandle =>
      val registered = shuffleApi.getShuffles().getShuffleIds()
      registered != null && registered.contains(CelebornExchangeStore.shuffleIdKey(h))
    case other => throw new IllegalArgumentException(s"not a CelebornHandle: $other")
  }

  /** A same-ID caller is unsafe against vanilla Celeborn because there is no driver-epoch fence.
    * A different ID is namespace-isolated but still cannot read the producer's shuffle keys, so
    * [[reattach]] refuses it separately. Production recovery needs the producer's stable logical
    * namespace plus a newer fenced epoch; changing only the application ID is not recovery. */
  override def checkIdentityIsolation(handle: ExchangeHandle): IsolationResult = handle match {
    case CelebornHandle(producingAppUniqueId, _) =>
      if (producingAppUniqueId == callerAppUniqueId) {
        IsolationConflict(
          s"resuming driver's own appUniqueId ('$callerAppUniqueId') is IDENTICAL to the " +
            "appUniqueId this anchor's data was produced under -- reattaching would risk " +
            "colliding with that run's own wire state rather than cleanly reading its " +
            "committed output; vanilla Celeborn has no driver-epoch fence. A distinct " +
            "appUniqueId is isolated but cannot address these shuffle keys either")
      } else {
        IsolationOk
      }
    case other => throw new IllegalArgumentException(s"not a CelebornHandle: $other")
  }

  override def reattach(handle: ExchangeHandle): ReattachResult =
    throw new UnsupportedOperationException(
      "CelebornExchangeStore.reattach: vanilla Apache Celeborn's public client API has no way " +
        "to read a shuffle's committed partition locations under an application identity other " +
        "than the one that registered it (ShuffleClient.readPartition/registerMapPartitionTask " +
        "are scoped to the registering LifecycleManager) -- this is docs/DESIGN.md §8's Tier 3, " +
        "a documented backend-patch requirement, not a bug in this store. See CelebornExchangeStore's doc comment.")

  /** Same Tier 3 gap as [[reattach]], not a separate one: `store` would need to write shuffle
    * data through Celeborn's real data-plane client (`LifecycleManager`/`ShuffleClient`) as this
    * project's OWN producer, which this project deliberately does not build here -- see
    * `docs/COMPATIBILITY.md`: `spark-resume-fs` is this repo's real `store`/`readPartition`
    * backend, `spark-resume-celeborn` stays metadata-only. Not attempted, not silently degraded. */
  override def store(partitions: Array[Array[Byte]]): ExchangeHandle =
    throw new UnsupportedOperationException(
      "CelebornExchangeStore.store: this store is metadata-only (see its own doc comment) -- " +
        "it never writes shuffle bytes through Celeborn's data-plane client. This is the same " +
        "Tier 3 gap as reattach, not a separate one. See CelebornExchangeStore's doc comment.")

  override def readPartition(handle: ExchangeHandle, partitionId: Int): Array[Byte] =
    throw new UnsupportedOperationException(
      "CelebornExchangeStore.readPartition: this store is metadata-only (see its own doc " +
        "comment) -- it never reads shuffle bytes through Celeborn's data-plane client. This is " +
        "the same Tier 3 gap as reattach, not a separate one. See CelebornExchangeStore's doc comment.")
}

object CelebornExchangeStore {

  /** The id format the master's `ShuffleApi.getShuffles()` reports shuffles under -- confirmed
    * against a real running master, not assumed (see `CelebornExchangeStoreSpec`). */
  private[celeborn] def shuffleIdKey(h: CelebornHandle): String = s"${h.appUniqueId}-${h.shuffleId}"
}
