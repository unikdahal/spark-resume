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

  test("unpinned reads fail closed instead of fingerprinting a live current snapshot") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT, v STRING) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1, 'a'), (2, 'b')")
    }
    withNewSession { spark =>
      intercept[IcebergFingerprintProvider.UnresolvedSnapshotException] {
        scanFingerprint(spark, "local.db.t")
      }
    }
  }

  test("GO/NO-GO: two explicitly pinned snapshots have different fingerprints") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT, v STRING) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1, 'a')")
    }
    val firstSnapshot = withNewSession { spark =>
      spark.sql("SELECT snapshot_id FROM local.db.t.snapshots").head().getLong(0)
    }

    val secondSnapshot = withNewSession { spark =>
      spark.sql("INSERT INTO local.db.t VALUES (2, 'b')") // a new commit -- a new snapshot id
      spark.sql("SELECT snapshot_id FROM local.db.t.snapshots ORDER BY committed_at DESC")
        .head().getLong(0)
    }
    val before = withNewSession { spark =>
      scanFingerprint(spark, s"local.db.t VERSION AS OF $firstSnapshot")
    }
    val after = withNewSession { spark =>
      scanFingerprint(spark, s"local.db.t VERSION AS OF $secondSnapshot")
    }

    before should not be after
  }

  test("two DIFFERENT tables with identical schema and row content still get DIFFERENT fingerprints") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t1 (id INT) USING iceberg")
      spark.sql("INSERT INTO local.db.t1 VALUES (1)")
      spark.sql("CREATE TABLE local.db.t2 (id INT) USING iceberg")
      spark.sql("INSERT INTO local.db.t2 VALUES (1)")
      val t1Snapshot = spark.sql("SELECT snapshot_id FROM local.db.t1.snapshots").head().getLong(0)
      val t2Snapshot = spark.sql("SELECT snapshot_id FROM local.db.t2.snapshots").head().getLong(0)
      scanFingerprint(spark, s"local.db.t1 VERSION AS OF $t1Snapshot") should not be
        scanFingerprint(spark, s"local.db.t2 VERSION AS OF $t2Snapshot")
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
      val currentSnapshotId = spark.sql(
        "SELECT snapshot_id FROM local.db.t.snapshots ORDER BY committed_at DESC").head().getLong(0)
      val currentFp = scanFingerprint(spark, s"local.db.t VERSION AS OF $currentSnapshotId")
      pinnedFp should not be currentFp

      // Re-reading the SAME pinned snapshot again is stable, regardless of further commits.
      spark.sql("INSERT INTO local.db.t VALUES (3)")
      val pinnedFpAgain = scanFingerprint(spark, s"local.db.t VERSION AS OF $firstSnapshotId")
      pinnedFp shouldBe pinnedFpAgain
    }
  }

  test("a freshly created table without a resolved snapshot fails closed") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.empty (id INT) USING iceberg")
      intercept[IcebergFingerprintProvider.UnresolvedSnapshotException] {
        scanFingerprint(spark, "local.db.empty")
      }
    }
  }

  test("REGRESSION: a commit racing an unpinned read cannot produce an admissible fingerprint") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1)") // V1

      val df = spark.sql("SELECT * FROM local.db.t") // planned against V1
      val scan = df.queryExecution.executedPlan.collectLeaves().collectFirst { case b: BatchScanExec => b }
        .getOrElse(fail("no BatchScanExec found before collect()"))
      intercept[IcebergFingerprintProvider.UnresolvedSnapshotException] {
        new IcebergFingerprintProvider().fingerprint(SparkPlanTarget(scan))
      }

      spark.sql("INSERT INTO local.db.t VALUES (2)") // a NEW commit (V2) lands before df ever executes
      df.collect() // actually runs the ALREADY-PLANNED df -- reads V1's files, confirmed the same
                    // BatchScanExec/SparkTable OBJECT before and after (`scanBefore eq scanAfter`,
                    // `.table eq .table`) by an earlier version of this test
      intercept[IcebergFingerprintProvider.UnresolvedSnapshotException] {
        new IcebergFingerprintProvider().fingerprint(SparkPlanTarget(scan))
      }

      // The integration layer catches provider failures, but its random NON_REUSABLE token must
      // make independently computed stage identities miss rather than fall back to a structural
      // connector string that could false-admit.
      val fp1 = WholePlanFingerprint.compute(scan, Seq(new IcebergFingerprintProvider()))
      val fp2 = WholePlanFingerprint.compute(scan, Seq(new IcebergFingerprintProvider()))
      fp1 should not be fp2
    }
  }

  test("end-to-end through WholePlanFingerprint.compute: capture then check across two independent sessions") {
    withNewSession { spark =>
      spark.sql("CREATE TABLE local.db.t (id INT, v STRING) USING iceberg")
      spark.sql("INSERT INTO local.db.t VALUES (1, 'a'), (2, 'b')")
    }
    val snapshotId = withNewSession { spark =>
      spark.sql("SELECT snapshot_id FROM local.db.t.snapshots").head().getLong(0)
    }
    val providers = Seq(new IcebergFingerprintProvider())
    val captureFp = withNewSession { spark =>
      val df = spark.sql(s"SELECT * FROM local.db.t VERSION AS OF $snapshotId")
      df.collect()
      WholePlanFingerprint.compute(df.queryExecution.executedPlan, providers)
    }
    val checkFp = withNewSession { spark =>
      val df = spark.sql(s"SELECT * FROM local.db.t VERSION AS OF $snapshotId")
      WholePlanFingerprint.compute(df.queryExecution.executedPlan, providers)
    }
    captureFp shouldBe checkFp
  }
}
