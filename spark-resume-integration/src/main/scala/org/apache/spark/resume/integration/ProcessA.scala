package org.apache.spark.resume.integration

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier

import org.apache.spark.sql.SparkSession

import org.apache.spark.resume.api._
import org.apache.spark.resume.celeborn.{CelebornExchangeStore, CelebornHandle}
import org.apache.spark.resume.fs.FsExchangeStore
import org.apache.spark.resume.redis.RedisAnchorStore
import org.apache.spark.resume.spark35.{StageCaptureListener, StageFingerprint}

/** The "producing" side of this module's cross-process, cross-backend composition proof (see
  * README.md). Runs a real Spark shuffle query to completion, real-fingerprints its shuffle
  * stage(s) via [[StageFingerprint.capturedStages]] (the exact function
  * `StageCaptureListener.onSuccess` itself calls -- see that class), registers ONE real Celeborn
  * shuffle to back it (the same real, engine-agnostic `LifecycleManager`/`ShuffleClient` API
  * `spark-resume-celeborn`'s own conformance fixtures use), and writes one real `Anchor` per
  * captured stage to a real Redis, carrying that REAL `CelebornHandle` -- NOT the
  * `StageCaptureListener.NoHandleKind` sentinel Phase 2 ships.
  *
  * Deliberately does not reuse `StageCaptureListener` itself: that class is Phase 2's own
  * committed, already-proven capture path, and it is CORRECT to always write the sentinel handle,
  * because `spark-resume-spark-3.5` genuinely has no way to know what Celeborn shuffle id (or any
  * other backend's handle) a given Spark shuffle stage maps to -- there is no producer-side
  * "issue me a handle" method on `ExchangeStore` at all (see that trait: every method is a
  * resuming-driver operation). Bridging that gap for real would mean either adding such a method
  * to the SPI, or wiring `spark-resume-spark-3.5` to a specific shuffle manager's internal id
  * scheme -- both real design decisions belonging to a future phase, not this one. This process
  * instead builds the real `CelebornHandle` itself, exactly the way an operator manually wiring
  * these two systems together today would have to. See README.md's "What this proves, and what it
  * doesn't" section.
  *
  * `INTEGRATION_SCENARIO` (default `"admitted"`) selects which real, disclosed outcome this run
  * is proving composes end to end (see `ProcessB`'s doc comment for the full outcome-by-scenario
  * table): every scenario other than `"stale"` registers a REAL Celeborn shuffle exactly the same
  * way. `"stale"` deliberately does NOT -- it writes an anchor carrying a `CelebornHandle` that
  * was never registered at all, so `ProcessB`'s `SafeReattach.attempt` reaches `RefusedStale` via
  * the backend's own real `isFresh` check, not a fabricated/mocked one. */
object ProcessA {

  def main(args: Array[String]): Unit = {
    val queryId = requireEnv("INTEGRATION_QUERY_ID")
    val scenario = sys.env.getOrElse("INTEGRATION_SCENARIO", "admitted")

    if (scenario == "skip") {
      runSkipScenario(queryId)
    } else {
      runCelebornBackedScenario(queryId, scenario)
    }
  }

  /** `scenario=skip`: the REAL producer-side capture path (`StageCaptureListener` itself,
    * registered as an actual listener -- not the manual anchor-building every other scenario in
    * this object does to fabricate a Celeborn handle), writing REAL row bytes through a real,
    * cross-process-durable `spark-resume-fs` store. This is the genuine cross-process proof
    * `ExecutionSkipAcceptanceSpec` (`spark-resume-spark-3.5`) cannot provide on its own: that
    * test uses `InMemoryExchangeStore`, real but explicitly NOT cross-process-durable (see its
    * own doc comment) -- this scenario proves the SAME mechanism survives an actual OS process
    * boundary, backed by real files on disk. See [[ProcessB]]'s doc comment for the resuming
    * side and the acceptance criteria. */
  private def runSkipScenario(queryId: String): Unit = {
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val fsBaseDir = requireEnv("FS_STORE_BASE_DIR")

    val redisStore = new RedisAnchorStore(redisHost, redisPort)
    val exchangeStore = new FsExchangeStore(fsBaseDir)
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-resume-integration-process-a-skip")
      .getOrCreate()
    try {
      val listener = new StageCaptureListener(queryId, redisStore, Seq.empty, exchangeStore = Some(exchangeStore))
      spark.listenerManager.register(listener)
      Fixture.query(spark).collect()
      // Two waits, both required and neither redundant: `waitUntilEmpty` gets the onSuccess event
      // DELIVERED, `awaitCaptures` gets the capture it queued FINISHED. StageCaptureListener does
      // its byte capture off the listener bus thread on purpose (see its doc comment) -- and this
      // process exits, and stops its SparkSession, immediately after this block, so anything still
      // queued would be lost with nothing to say so.
      spark.sparkContext.listenerBus.waitUntilEmpty(30000)
      require(listener.awaitCaptures(120000),
        "StageCaptureListener's captures did not finish within 120s -- ProcessB would read anchors " +
          "this process never finished writing")
      listener.close()

      val anchors = redisStore.loadAnchors(StageCaptureListener.stageQueryId(queryId))
      require(anchors.nonEmpty, "StageCaptureListener wrote no anchors -- fixture bug, not a real finding")
      require(anchors.forall(_.handleKind == exchangeStore.handleKind),
        s"expected every anchor to carry a REAL fs handle, got handleKinds: ${anchors.map(_.handleKind)}")
      println(s"[ProcessA] scenario=skip: captured ${anchors.size} real anchor(s) with REAL row bytes " +
        s"via StageCaptureListener, stored under $fsBaseDir")
    } finally {
      redisStore.close()
      spark.stop()
    }
  }

  private def runCelebornBackedScenario(queryId: String, scenario: String): Unit = {
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = sys.env.getOrElse("REDIS_PORT", "6379").toInt
    val celebornRestBaseUrl = sys.env.getOrElse("CELEBORN_MASTER_REST", "http://localhost:9098")
    val celebornMasterEndpoint = sys.env.getOrElse("CELEBORN_MASTER_ENDPOINT", "localhost:9097")
    val producingAppUniqueId = s"integration-producer-$queryId"

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-resume-integration-process-a")
      .getOrCreate()

    try {
      val df = Fixture.query(spark)
      df.collect() // materialize the real shuffle
      val stages = StageFingerprint.capturedStages(df.queryExecution.executedPlan, Seq.empty)
      require(stages.nonEmpty, "fixture query produced no shuffle stage -- fixture bug, not a real finding")
      println(s"[ProcessA] captured ${stages.size} real shuffle stage(s), scenario=$scenario")

      // masterRestBaseUrl only, here: this store is used purely to get at handleKind/
      // serializeHandle, both of which don't touch the "caller identity" constructor arg at all --
      // that arg only matters for checkIdentityIsolation, a RESUMING-driver-side check ProcessA
      // never calls.
      val celebornStore = new CelebornExchangeStore(celebornRestBaseUrl, s"unused-producer-side-$queryId")
      val handle =
        if (scenario == "stale") {
          // Deliberately never registered anywhere -- see this object's doc comment.
          println("[ProcessA] scenario=stale: NOT registering a real shuffle; handle points at one that never existed")
          CelebornHandle(s"never-registered-$queryId", shuffleId = 1)
        } else {
          registerRealCelebornShuffle(producingAppUniqueId, celebornMasterEndpoint)
          CelebornHandle(producingAppUniqueId, shuffleId = 1)
        }
      val handlePayload = celebornStore.serializeHandle(handle)

      val redisStore = new RedisAnchorStore(redisHost, redisPort)
      try {
        val stageQueryId = StageCaptureListener.stageQueryId(queryId)
        val generation = redisStore.acquireGeneration(stageQueryId)
        stages.foreach { s =>
          val anchor = Anchor(
            schemaVersion = "1",
            queryId = stageQueryId,
            generation = generation,
            fingerprint = s.digest,
            handleKind = celebornStore.handleKind,
            handlePayload = handlePayload,
            numMappers = s.numMappers,
            numPartitions = s.numPartitions,
            bytesByPartition = s.bytesByPartition,
            rowCount = 0L,
            createdAtMs = System.currentTimeMillis())
          val ok = redisStore.putAnchor(generation, anchor)
          require(ok, "putAnchor refused under a freshly acquired generation -- should be impossible")
        }
        println(s"[ProcessA] wrote ${stages.size} anchor(s) carrying handle=$handle " +
          s"to Redis under queryId=$stageQueryId, generation=$generation")
      } finally {
        redisStore.close()
      }
    } finally {
      spark.stop()
    }
  }

  private def registerRealCelebornShuffle(appUniqueId: String, masterEndpoint: String): Unit = {
    val conf = new CelebornConf()
    conf.set("celeborn.master.endpoints", masterEndpoint)
    val lifecycleManager = new LifecycleManager(appUniqueId, conf)
    // Deliberately NOT stopped -- confirmed empirically (spark-resume-celeborn's
    // CelebornExchangeStoreSpec / SafeReattachIntegrationSpec) that LifecycleManager.stop()
    // ACTIVELY deregisters the shuffle at the master, which would invalidate this fixture before
    // ProcessB (a separate, later-launched JVM) gets to use it.
    val client = ShuffleClient.get(
      appUniqueId, lifecycleManager.getHost, lifecycleManager.getPort, conf,
      UserIdentifier.apply("spark-resume-integration", "process-a"))
    client.registerMapPartitionTask(1, /* numMappers */ 1, /* mapId */ 0, /* attemptId */ 0, /* numPartitions */ 1)
  }

  private def requireEnv(name: String): String =
    sys.env.getOrElse(name, throw new IllegalStateException(s"required env var $name not set"))
}
