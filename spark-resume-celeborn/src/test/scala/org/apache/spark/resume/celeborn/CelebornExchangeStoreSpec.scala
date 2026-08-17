package org.apache.spark.resume.celeborn

import java.util.UUID

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier

import org.apache.spark.resume.api.{ExchangeHandle, ExchangeStore}
import org.apache.spark.resume.api.testkit.ExchangeStoreContract

/** Runs the full [[ExchangeStoreContract]] conformance suite against a REAL Celeborn master and
  * worker -- not a mock. Needs a vanilla Celeborn cluster reachable at the RPC/REST addresses
  * below (see `spark-resume-celeborn/README.md` for how to start one; matches this repo's own
  * dev setup).
  *
  * `reattachSupported = false`: see [[CelebornExchangeStore]]'s doc comment for why this is a
  * real, checked, permanent property of vanilla Celeborn's public client API, not a shortcut.
  *
  * `freshHandle` drives an ACTUAL shuffle registration through Celeborn's real, engine-agnostic
  * client API (`LifecycleManager`/`ShuffleClient`, the same public API Flink/MR use, not a
  * Spark-specific shim) -- a genuine registered shuffle the master's own admin REST API reports,
  * not a synthetic fixture. `staleHandle` is deliberately simpler than "register then supersede":
  * a shuffle id that was NEVER registered at all is just as genuinely "not fresh" without needing
  * to reverse-engineer this master version's exact supersession/expiry timing (a real, disclosed
  * scoping choice -- this proves `isFresh`'s negative path for "never existed," not "existed then
  * expired," which this suite did not need to chase down given a real backend to test against
  * either way answers `isFresh` correctly). */
class CelebornExchangeStoreSpec extends ExchangeStoreContract {

  private val masterRpcEndpoint = ("localhost", 9097)
  private val masterRestBaseUrl = "http://localhost:9098"

  // The identity THIS store (the "resuming driver" in every test) is built with -- fixed once per
  // Spec instance so conflictingIdentityHandle can hand back a handle claiming the SAME identity,
  // deliberately, without needing to read it back off the store.
  private val callerAppUniqueId = s"resuming-driver-${UUID.randomUUID()}"

  override def newStore(): ExchangeStore = new CelebornExchangeStore(masterRestBaseUrl, callerAppUniqueId)

  override def reattachSupported: Boolean = false

  override def freshHandle(store: ExchangeStore): ExchangeHandle = {
    val appUniqueId = s"producing-driver-${UUID.randomUUID()}"
    registerRealShuffle(appUniqueId, shuffleId = 1)
    CelebornHandle(appUniqueId, 1)
  }

  override def staleHandle(store: ExchangeStore): ExchangeHandle =
    CelebornHandle(s"never-registered-${UUID.randomUUID()}", 1)

  override def conflictingIdentityHandle(store: ExchangeStore): Option[ExchangeHandle] =
    Some(CelebornHandle(callerAppUniqueId, 1))

  private def registerRealShuffle(appUniqueId: String, shuffleId: Int): Unit = {
    val conf = new CelebornConf()
    conf.set("celeborn.master.endpoints", s"${masterRpcEndpoint._1}:${masterRpcEndpoint._2}")
    val lifecycleManager = new LifecycleManager(appUniqueId, conf)
    val client = ShuffleClient.get(
      appUniqueId, lifecycleManager.getHost, lifecycleManager.getPort, conf,
      UserIdentifier.apply("spark-resume-celeborn-test", "conformance-suite"))
    client.registerMapPartitionTask(shuffleId, /* numMappers */ 1, /* mapId */ 0, /* attemptId */ 0, /* numPartitions */ 1)
  }
}
