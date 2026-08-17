package org.apache.spark.resume.api.testkit

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.api.{Anchor, AnchorStore}

/** Shared conformance suite for any [[AnchorStore]] implementation. Extend this, implement
  * `newStore()`, and every test below runs against your implementation for free -- per
  * docs/DESIGN.md §12, this is a hard requirement for any implementation this project accepts as
  * proven, in-tree or out-of-tree.
  *
  * Ships here, in `spark-resume-api`'s test-jar, specifically so a downstream module never has to
  * duplicate it: depend on `spark-resume-api_2.12:test-jar:tests` (see `spark-resume-core/pom.xml`
  * for the exact coordinates) and extend this class. */
abstract class AnchorStoreContract extends AnyFunSuite with Matchers {

  /** A fresh, empty store instance -- called once per test, so tests must not depend on state
    * left over from a previous test. */
  def newStore(): AnchorStore

  private def anchor(queryId: String, generation: Long, fingerprint: String): Anchor =
    Anchor(
      schemaVersion = "1",
      queryId = queryId,
      generation = generation,
      fingerprint = fingerprint,
      handleKind = "test",
      handlePayload = fingerprint.getBytes("UTF-8"),
      numMappers = 1,
      numPartitions = 1,
      bytesByPartition = Array(1L),
      rowCount = 1L,
      createdAtMs = System.currentTimeMillis())

  test("acquireGeneration's first value is distinguishable from a never-acquired queryId, and it is monotonic") {
    val store = newStore()
    val g1 = store.acquireGeneration("q1")
    // 0 is the conventional "never written" sentinel a fence-gated putAnchor check would compare
    // against for a queryId nothing has called acquireGeneration for yet -- the first real
    // generation must not collide with it, or a caller could not tell "never acquired" apart from
    // "acquired once."
    g1 should be > 0L
    val g2 = store.acquireGeneration("q1")
    val g3 = store.acquireGeneration("q1")
    g2 should be > g1
    g3 should be > g2
  }

  test("acquireGeneration is independent per queryId") {
    val store = newStore()
    val a1 = store.acquireGeneration("qa")
    val b1 = store.acquireGeneration("qb")
    val a2 = store.acquireGeneration("qa")
    val b2 = store.acquireGeneration("qb")
    a2 should be > a1
    b2 should be > b1
  }

  test("putAnchor with the CURRENT generation succeeds and the anchor is loadable") {
    val store = newStore()
    val gen = store.acquireGeneration("q1")
    val a = anchor("q1", gen, "fp-1")
    store.putAnchor(gen, a) shouldBe true
    store.loadAnchors("q1").map(_.fingerprint) should contain("fp-1")
  }

  test("putAnchor with a STALE generation is refused, and does not corrupt existing anchors") {
    val store = newStore()
    val gen1 = store.acquireGeneration("q1")
    store.putAnchor(gen1, anchor("q1", gen1, "fp-first")) shouldBe true

    // A new writer supersedes gen1 -- the zombie-writer scenario: gen1 is now stale.
    val gen2 = store.acquireGeneration("q1")
    gen2 should be > gen1

    // The old (now-zombie) writer tries to write under its stale generation. Must be refused.
    val refused = store.putAnchor(gen1, anchor("q1", gen1, "fp-from-zombie-writer"))
    refused shouldBe false

    // The zombie's write must not be visible, and the original anchor must be untouched.
    val loaded = store.loadAnchors("q1")
    loaded.map(_.fingerprint) should not contain "fp-from-zombie-writer"
    loaded.map(_.fingerprint) should contain("fp-first")

    // The current writer can still write under the current generation.
    store.putAnchor(gen2, anchor("q1", gen2, "fp-second")) shouldBe true
    store.loadAnchors("q1").map(_.fingerprint) should contain("fp-second")
  }

  test("loadAnchors on a queryId nothing was ever written for returns empty, not an error") {
    val store = newStore()
    store.loadAnchors("never-seen") shouldBe empty
  }

  test("acquireGeneration is a real fence under CONCURRENT callers, not just sequential ones") {
    // Sequential calls passing this test proves nothing about a real multi-writer race -- this
    // is the scenario a naive read-then-increment (non-atomic) implementation fails, only under
    // genuine concurrent pressure, which is why this uses real threads, not a loop.
    val store = newStore()
    val threads = 16
    val perThread = 200
    val pool = Executors.newFixedThreadPool(threads)
    val start = new CountDownLatch(1)
    val seen = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
    val errors = new AtomicInteger(0)
    try {
      val futures = (0 until threads).map { _ =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            try {
              start.await()
              (0 until perThread).foreach(_ => seen.add(store.acquireGeneration("concurrent-q")))
            } catch {
              case _: Throwable => errors.incrementAndGet()
            }
          }
        })
      }
      start.countDown()
      futures.foreach(_.get(30, TimeUnit.SECONDS))
    } finally {
      pool.shutdown()
    }
    errors.get() shouldBe 0
    val values = seen.toArray.map(_.asInstanceOf[Long])
    values.length shouldBe threads * perThread
    // The whole point of the fence: no two concurrent callers ever observed the same value --
    // deliberately NOT asserting a specific starting value or a gap-free sequence here, since the
    // interface only promises monotonicity and uniqueness, not a numbering scheme.
    values.toSet.size shouldBe values.length
  }

  test("a losing writer's anchor never becomes visible even under concurrent writers racing the fence") {
    val store = newStore()
    val gen1 = store.acquireGeneration("race-q")
    val gen2 = store.acquireGeneration("race-q") // supersedes gen1 before either writer writes
    val results = mutable.ArrayBuffer.empty[Boolean]
    val pool = Executors.newFixedThreadPool(2)
    try {
      val f1 = pool.submit(new Runnable {
        override def run(): Unit =
          results.synchronized(results += store.putAnchor(gen1, anchor("race-q", gen1, "loser")))
      })
      val f2 = pool.submit(new Runnable {
        override def run(): Unit =
          results.synchronized(results += store.putAnchor(gen2, anchor("race-q", gen2, "winner")))
      })
      f1.get(10, TimeUnit.SECONDS)
      f2.get(10, TimeUnit.SECONDS)
    } finally {
      pool.shutdown()
    }
    val loaded = store.loadAnchors("race-q")
    loaded.map(_.fingerprint) should contain("winner")
    loaded.map(_.fingerprint) should not contain "loser"
  }
}
