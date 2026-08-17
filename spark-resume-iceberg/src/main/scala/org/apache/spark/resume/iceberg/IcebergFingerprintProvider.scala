package org.apache.spark.resume.iceberg

import java.security.MessageDigest

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.spark.resume.api.{FingerprintTarget, SourceFingerprint}

/** [[SourceFingerprint]] for an Iceberg table read through Spark's DSv2 path
  * (`BatchScanExec` wrapping Iceberg's own `SparkTable`/`SparkBatchQueryScan`) -- a genuinely
  * different, cheaper identity model from [[org.apache.spark.resume.spark35.FileSourceFingerprint]]'s
  * file-listing approach: Iceberg's own committed SNAPSHOT ID already IS the immutable
  * point-in-time table identity a query resolved against, so fingerprinting it is exact and
  * requires no file enumeration (no extra storage round-trip beyond what planning the query
  * already did).
  *
  * Getting to that snapshot id took real investigation, not a documentation lookup, because
  * neither `Scan.description()` nor `Scan.toString()` include it (both verified empirically to
  * return the identical string before and after a committing INSERT -- see this module's test
  * suite's `DebugIcebergPlanSpec`-derived findings, folded into `IcebergFingerprintProviderSpec`
  * rather than kept as a separate throwaway), and `SparkBatchQueryScan.snapshotId()` -- the field
  * that DOES hold it -- is package-private to `org.apache.iceberg.spark.source`, unreachable
  * without reflection into a third-party connector's internals. This project's own posture (see
  * docs/DESIGN.md sec 8: Tier 1 is public API only, no silent internals-reaching) rules that out.
  * The path that IS public: `BatchScanExec.table` (public, Spark's own DSv2 `Table` type) can be
  * cast to `org.apache.iceberg.spark.source.SparkTable` -- a genuinely PUBLIC Iceberg class
  * (Iceberg's own catalog-facing DSv2 `Table` implementation, meant to be visible) -- whose public
  * `snapshotId()`/`branch()`/`table()` accessors give everything needed.
  *
  * Resolution order, checked in this priority because a query can be pinned three different ways
  * and only one is ever active at once:
  *   1. `branch()` non-null (the query read a named branch, e.g. `SELECT * FROM t.branch_x`) --
  *      resolved via `Table.snapshot(branchName)`, NOT fingerprinted as the branch NAME itself.
  *      A branch is a MOVING pointer; fingerprinting the name would make two reads of a branch
  *      whose tip advanced between them collide on an identical fingerprint despite reading
  *      different data -- exactly the false-positive-resumption hazard invariant A-1 forbids.
  *   2. `snapshotId()` non-null (the query pinned an exact snapshot or timestamp, e.g.
  *      `VERSION AS OF <id>`) -- used directly; this is already the resolved point-in-time
  *      identity the query itself chose.
  *   3. Neither set -- an ordinary unpinned read of the table's main branch -- resolved via
  *      `Table.currentSnapshot()`, read once, at the moment this method is called.
  * A table with no snapshot at all yet (created but never written to) resolves to
  * [[IcebergFingerprintProvider.EmptyTableSentinel]] rather than crashing or hashing `null`. */
final class IcebergFingerprintProvider extends SourceFingerprint {

  override def supports(target: FingerprintTarget): Boolean = target.node match {
    case scan: BatchScanExec => scan.table.isInstanceOf[SparkTable]
    case _ => false
  }

  override def fingerprint(target: FingerprintTarget): String = {
    val scan = target.node.asInstanceOf[BatchScanExec]
    val table = scan.table.asInstanceOf[SparkTable]
    IcebergFingerprintProvider.sha256Hex(s"${table.name()}|${resolveSnapshotId(table)}")
  }

  private def resolveSnapshotId(table: SparkTable): String = {
    val branch = table.branch()
    val pinned = table.snapshotId()
    if (branch != null) {
      table.table().snapshot(branch) match {
        case null => IcebergFingerprintProvider.EmptyTableSentinel
        case snap => snap.snapshotId().toString
      }
    } else if (pinned != null) {
      pinned.toString
    } else {
      table.table().currentSnapshot() match {
        case null => IcebergFingerprintProvider.EmptyTableSentinel
        case snap => snap.snapshotId().toString
      }
    }
  }
}

object IcebergFingerprintProvider {

  private val EmptyTableSentinel = "EMPTY-NO-SNAPSHOT-YET"

  private def sha256Hex(s: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
}
