package org.apache.spark.resume.celeborn

import java.util.UUID

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.core._

/** The end-to-end proof this whole module exists to deliver: `spark-resume-core`'s
  * `SafeReattach.attempt` -- the ONE enforced choke point every driver-side integration in this
  * project calls `ExchangeStore.reattach` through -- run against a REAL `CelebornExchangeStore`
  * pointed at a REAL Celeborn cluster, not a mock of either side. */
class SafeReattachIntegrationSpec extends AnyFunSuite with Matchers {

  private def registerRealShuffle(appUniqueId: String, shuffleId: Int): Unit = {
    val conf = new CelebornConf()
    conf.set("celeborn.master.endpoints", "localhost:9097")
    val lifecycleManager = new LifecycleManager(appUniqueId, conf)
    val client = ShuffleClient.get(
      appUniqueId, lifecycleManager.getHost, lifecycleManager.getPort, conf,
      UserIdentifier.apply("spark-resume-celeborn-test", "safereattach-suite"))
    client.registerMapPartitionTask(shuffleId, 1, 0, 0, 1)
  }

  test("a fresh, isolation-ok, REALLY-registered handle -> RefusedUnsupported, not a raw throw, not a fake success") {
    val producingAppUniqueId = s"producing-${UUID.randomUUID()}"
    registerRealShuffle(producingAppUniqueId, shuffleId = 1)
    val handle = CelebornHandle(producingAppUniqueId, 1)

    // The resuming driver's OWN identity, deliberately different from the producing run's --
    // the real hazard checkIdentityIsolation exists to catch (see CelebornExchangeStore's doc
    // comment).
    val store = new CelebornExchangeStore("http://localhost:9098", s"resuming-${UUID.randomUUID()}")

    SafeReattach.attempt(store, handle) match {
      case RefusedUnsupported(reason) =>
        reason should include("application identity other than the one that registered it")
      case other =>
        fail(s"expected RefusedUnsupported (the real, checked Tier 3 gap), got $other")
    }
  }

  test("a handle for a shuffle that was NEVER registered -> RefusedStale, reattach never even attempted") {
    val store = new CelebornExchangeStore("http://localhost:9098", s"resuming-${UUID.randomUUID()}")
    val handle = CelebornHandle(s"never-existed-${UUID.randomUUID()}", 1)

    SafeReattach.attempt(store, handle) shouldBe RefusedStale
  }

  test("a resuming driver launched under the SAME appUniqueId as the producing run -> RefusedIsolationConflict, reattach never even attempted") {
    val sharedAppUniqueId = s"reused-identity-${UUID.randomUUID()}"
    registerRealShuffle(sharedAppUniqueId, shuffleId = 1)
    val handle = CelebornHandle(sharedAppUniqueId, 1)

    // The misconfiguration this project's own prior empirical finding was about: a resuming
    // driver whose appUniqueId collides with the run that produced the anchor.
    val store = new CelebornExchangeStore("http://localhost:9098", sharedAppUniqueId)

    SafeReattach.attempt(store, handle) match {
      case RefusedIsolationConflict(reason) => reason should not be empty
      case other => fail(s"expected RefusedIsolationConflict, got $other")
    }
  }
}
