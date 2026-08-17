package org.apache.spark.resume.iceberg

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Shared local-mode Spark+Iceberg harness -- a fresh `SparkSession` per test, wired to a fresh
  * `HadoopCatalog`-backed warehouse directory per test, so tests don't share table state. Local
  * Hadoop catalog (not a real REST/Glue/Nessie catalog) is deliberate: it needs no external
  * infrastructure to run, matching this repo's whole approach to local-mode testing, and every
  * fingerprint claim this module makes is about SNAPSHOT identity, which is catalog-independent
  * (a REST catalog resolves to the identical `SparkTable`/snapshot-id shape once the DataFrame is
  * planned). */
trait IcebergTestBase extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var warehouse: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    warehouse = Files.createTempDirectory("spark-resume-iceberg-spec")
  }

  override def afterEach(): Unit = {
    deleteRecursively(warehouse.toFile)
    super.afterEach()
  }

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  def withNewSession[T](body: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .appName("spark-resume-iceberg-spec")
      .master("local[2]")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehouse.toString)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try body(spark) finally spark.stop()
  }
}
