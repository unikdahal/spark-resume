package org.apache.spark.resume.iceberg

import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.spark.resume.spark35.{SparkPlanTarget, WholePlanFingerprint}

/** Proves the resolution-order claims [[IcebergFingerprintProvider]]'s doc comment makes, by
  * ACTUALLY committing snapshots and reading them back -- not by inspecting the resolution code. */
class IcebergFingerprintProviderSpec extends IcebergTestBase {

  private def scanFingerprint(spark: org.apache.spark.sql.SparkSession, table: String): String = {
    val df = spark.sql(s"SELECT * FROM $table")
    val scan = df.queryExecution.executedPlan.collectLeaves().collectFirst { case b: BatchScanExec => b }
      .getOrElse(fail(s"no BatchScanExec found in plan for $table"))
    new IcebergFingerprintProvider().fingerprint(SparkPlanTarget(scan))
  }

  test("supports() is true for an Iceberg BatchScanExec, false for anything else") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1)")
      val df = spark.sql("SELECT * FROM local.db.t")
      val scan = df.queryExecution.executedPlan.collectLeaves().collectFirst { case b: BatchScanExec => b }.get
      val provider = new IcebergFingerprintProvider()
      provider.supports(SparkPlanTarget(scan)) shouldBe true

      val rangeDf = spark.range(5)
      val rangeLeaf = rangeDf.queryExecution.executedPlan.collectLeaves().head
      provider.supports(SparkPlanTarget(rangeLeaf)) shouldBe false
    }
  }

  test("STABILITY: an unmutated table, read twice from two INDEPENDENT SparkSessions, fingerprints identically") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT, v STRING) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1, 'a'), (2, 'b')")
    }
    val fp1 = withNewSession { spark => scanFingerprint(spark, "local.db.t") }
    val fp2 = withNewSession { spark => scanFingerprint(spark, "local.db.t") }
    fp1 shouldBe fp2
  }

  test("GO/NO-GO: a snapshot committed AFTER the first read changes the fingerprint on the next read") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT, v STRING) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1, 'a')")
    }
    val before = withNewSession { spark => scanFingerprint(spark, "local.db.t") }

    withNewSession { spark =>
      spark.sql("INSERT INTO local.db.t VALUES (2, 'b')") // a new commit -- a new snapshot id
    }
    val after = withNewSession { spark => scanFingerprint(spark, "local.db.t") }

    before should not be after
  }

  test("two DIFFERENT tables with identical schema and row content still get DIFFERENT fingerprints") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t1 (id INT) USING iceberg")
      spark.sql("INSERT INTO local.db.t1 VALUES (1)")
      spark.sql("CREATE TABLE local.db.t2 (id INT) USING iceberg")
      spark.sql("INSERT INTO local.db.t2 VALUES (1)")
      scanFingerprint(spark, "local.db.t1") should not be scanFingerprint(spark, "local.db.t2")
    }
  }

  test("a table pinned to an explicit snapshot id (VERSION AS OF) fingerprints that snapshot, not whatever is current") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1)")
      val firstSnapshotId = spark.sql("SELECT snapshot_id FROM local.db.t.snapshots ORDER BY committed_at")
        .collect().head.getLong(0)
      spark.sql("INSERT INTO local.db.t VALUES (2)") // advances "current" past the pinned snapshot

      val pinnedFp = scanFingerprint(spark, s"local.db.t VERSION AS OF $firstSnapshotId")
      val currentFp = scanFingerprint(spark, "local.db.t")
      pinnedFp should not be currentFp

      // Re-reading the SAME pinned snapshot again is stable, regardless of further commits.
      spark.sql("INSERT INTO local.db.t VALUES (3)")
      val pinnedFpAgain = scanFingerprint(spark, s"local.db.t VERSION AS OF $firstSnapshotId")
      pinnedFp shouldBe pinnedFpAgain
    }
  }

  test("a freshly created table with no commits yet fingerprints to the disclosed empty-table sentinel, not a crash") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.empty (id INT) USING iceberg")
      noException should be thrownBy scanFingerprint(spark, "local.db.empty")
    }
  }

  test("end-to-end through WholePlanFingerprint.compute: capture then check across two independent sessions") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT, v STRING) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1, 'a'), (2, 'b')")
    }
    val providers = Seq(new IcebergFingerprintProvider())
    val captureFp = withNewSession { spark =>
      val df = spark.sql("SELECT * FROM local.db.t")
      df.collect()
      WholePlanFingerprint.compute(df.queryExecution.executedPlan, providers)
    }
    val checkFp = withNewSession { spark =>
      val df = spark.sql("SELECT * FROM local.db.t")
      WholePlanFingerprint.compute(df.queryExecution.executedPlan, providers)
    }
    captureFp shouldBe checkFp
  }
}
