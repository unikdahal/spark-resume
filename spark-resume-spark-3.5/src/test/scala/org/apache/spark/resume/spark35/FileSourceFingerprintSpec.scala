package org.apache.spark.resume.spark35

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.FileSourceScanExec

class FileSourceFingerprintSpec extends SparkTestBase {

  private val provider = new FileSourceFingerprint

  /** Finds the (assumed single) `FileSourceScanExec` leaf in `plan`, unwrapping AQE wrappers --
    * a minimal, test-only version of the same unwrap `WholePlanFingerprint` does for real. */
  private def findFileScan(spark: SparkSession, sql: String): FileSourceScanExec = {
    import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
    def unwrap(p: org.apache.spark.sql.execution.SparkPlan): org.apache.spark.sql.execution.SparkPlan = p match {
      case a: AdaptiveSparkPlanExec => unwrap(a.executedPlan)
      case s: QueryStageExec => unwrap(s.plan)
      case other => other
    }
    val df = spark.sql(sql)
    df.collect() // force AQE to finalize, so executedPlan reflects the real, materialized plan
    def find(p: org.apache.spark.sql.execution.SparkPlan): Option[FileSourceScanExec] = {
      val u = unwrap(p)
      u match {
        case f: FileSourceScanExec => Some(f)
        case _ => u.children.flatMap(find).headOption
      }
    }
    find(df.queryExecution.executedPlan).getOrElse(
      fail(s"no FileSourceScanExec found in plan for: $sql"))
  }

  private def writeParquet(spark: SparkSession, dir: String, rows: Seq[(Int, String)]): Unit = {
    import spark.implicits._
    rows.toDF("id", "v").write.mode("overwrite").parquet(dir)
  }

  test("supports() is true for FileSourceScanExec, false for anything else") {
    withNewSession() { spark =>
      val dir = tmpDir.resolve("t1").toString
      writeParquet(spark, dir, Seq((1, "a"), (2, "b")))
      spark.read.parquet(dir).createOrReplaceTempView("t1")
      val scan = findFileScan(spark, "select * from t1")
      provider.supports(SparkPlanTarget(scan)) shouldBe true

      val rangeExec = spark.range(0, 3).queryExecution.executedPlan
      provider.supports(SparkPlanTarget(rangeExec)) shouldBe false
    }
  }

  test("STABILITY: identical files, identical query, two INDEPENDENT SparkSessions -> identical fingerprint") {
    val dir = tmpDir.resolve("stable").toString
    val fp1 = withNewSession() { spark =>
      writeParquet(spark, dir, Seq((1, "a"), (2, "b"), (3, "c")))
      spark.read.parquet(dir).createOrReplaceTempView("t")
      provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from t")))
    }
    val fp2 = withNewSession() { spark =>
      spark.read.parquet(dir).createOrReplaceTempView("t")
      provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from t")))
    }
    fp1 shouldBe fp2
  }

  test("NEGATIVE (go/no-go): rewriting the file's content AND length between runs changes the fingerprint") {
    val dir = tmpDir.resolve("mutate").toString
    val fpBefore = withNewSession() { spark =>
      writeParquet(spark, dir, Seq((1, "a"), (2, "b")))
      spark.read.parquet(dir).createOrReplaceTempView("t")
      provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from t")))
    }
    // A materially different row count/content -- not just a same-tick rewrite that a
    // length-and-content-blind check (or an mtime-granularity-blind one) could miss.
    val fpAfter = withNewSession() { spark =>
      writeParquet(spark, dir, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")))
      spark.read.parquet(dir).createOrReplaceTempView("t")
      provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from t")))
    }
    fpBefore should not be fpAfter
  }

  test("two DIFFERENT tables with identical schema and identical row content still get DIFFERENT fingerprints") {
    val dirA = tmpDir.resolve("tableA").toString
    val dirB = tmpDir.resolve("tableB").toString
    withNewSession() { spark =>
      writeParquet(spark, dirA, Seq((1, "x"), (2, "y")))
      writeParquet(spark, dirB, Seq((1, "x"), (2, "y")))
      spark.read.parquet(dirA).createOrReplaceTempView("ta")
      spark.read.parquet(dirB).createOrReplaceTempView("tb")
      val fpA = provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from ta")))
      val fpB = provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from tb")))
      // Different physical files (different paths), even with byte-identical content -- source
      // IDENTITY discriminates, not just content, which is what a real "same content, different
      // provenance" scenario needs.
      fpA should not be fpB
    }
  }

  test("PARTITIONED table with a partition-filtered query: fingerprint reflects only files actually read, and is stable") {
    val dir = tmpDir.resolve("partitioned").toString
    withNewSession() { spark =>
      import spark.implicits._
      Seq((1, "a", 2024), (2, "b", 2024), (3, "c", 2025))
        .toDF("id", "v", "year")
        .write.mode("overwrite").partitionBy("year").parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("p")

      val fp2024a = provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from p where year = 2024")))
      val fp2025 = provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from p where year = 2025")))
      // Different partitions read -> different file sets -> must differ.
      fp2024a should not be fp2025

      // Same partition-filtered query run again -> identical files listed -> stable.
      val fp2024b = provider.fingerprint(SparkPlanTarget(findFileScan(spark, "select * from p where year = 2024")))
      fp2024a shouldBe fp2024b
    }
  }

  test("A-1 SAFETY, confirmed NOT vulnerable to Iceberg's live-refresh race: the SAME scan node fingerprints identically before and after an on-disk overwrite") {
    // IcebergFingerprintProvider was found to have a real, confirmed A-1 gap here: the identical
    // BatchScanExec/SparkTable object returns a DIFFERENT (newer) snapshot id when asked again
    // post-execution, because its underlying Table handle auto-refreshes live. The obvious
    // question this raised: does FileSourceFingerprint -- the DEFAULT provider everyone uses --
    // have the same hole? Checked directly rather than left as an inference.
    withNewSession() { spark =>
      val dir = tmpDir.resolve("filerace").toString
      writeParquet(spark, dir, Seq((1, "a"), (2, "b")))
      val df = spark.read.parquet(dir)
      val scan = df.queryExecution.executedPlan.collectLeaves()
        .collectFirst { case f: FileSourceScanExec => f }
        .getOrElse(fail("no FileSourceScanExec found"))
      val fpBefore = provider.fingerprint(SparkPlanTarget(scan))

      // A materially different overwrite -- content, length, AND the underlying part-file names
      // themselves all change (parquet "overwrite" deletes and rewrites, it does not patch
      // in place).
      writeParquet(spark, dir, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")))

      // Same node OBJECT, asked again -- no new plan, no collect(). If FileSourceScanExec's
      // FileIndex re-listed storage live the way Iceberg's Table handle does, this would now
      // differ from fpBefore despite being the identical scan.
      val fpAfter = provider.fingerprint(SparkPlanTarget(scan))
      fpBefore shouldBe fpAfter

      // Confirms WHY, not just that: the file paths this scan was planned against are gone after
      // the overwrite (parquet overwrite renames, doesn't patch), so an actual execution against
      // this stale plan fails outright rather than silently reading new data -- corroborating
      // that FileSourceScanExec's InMemoryFileIndex is fixed at plan time, not live-refreshing.
      an[Exception] should be thrownBy df.collect()
    }
  }
}
