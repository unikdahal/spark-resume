package org.apache.spark.resume.spark35

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Shared local-mode Spark harness. Deliberately builds a FRESH `SparkSession` per test (not a
  * shared one) via `withNewSession` -- the stability tests specifically need to prove a
  * fingerprint survives across two INDEPENDENT sessions, not just two calls within one, since an
  * object-identity or exprId-drift bug can hide behind a same-JVM/same-session comparison. */
trait SparkTestBase extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var tmpDir: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tmpDir = Files.createTempDirectory("spark-resume-spec")
  }

  override def afterEach(): Unit = {
    deleteRecursively(tmpDir.toFile)
    super.afterEach()
  }

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  /** @param extraConf applied on the BUILDER, before the session exists -- not via `spark.conf.set`
    *   after `getOrCreate()` returns. `getOrCreate()` reuses an already-stopped session's cleared
    *   active/default-session slots to build a genuinely new session and (separately) a runtime
    *   `spark.conf.set` is itself already session-scoped SQLConf state, not global -- so a
    *   post-hoc `.set` here would likely already be safely session-local. Builder-time is used
    *   anyway wherever a test needs non-default config, so "does this leak to the next session"
    *   is never a question that needs answering per call site. */
  def withNewSession[T](adaptiveEnabled: Boolean = true, extraConf: Map[String, String] = Map.empty)
                        (body: SparkSession => T): T = {
    val builder = SparkSession.builder()
      .appName("spark-resume-spark35-spec")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", adaptiveEnabled.toString)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
    val spark = extraConf.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }.getOrCreate()
    try body(spark) finally spark.stop()
  }
}
