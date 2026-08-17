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
  * [[IcebergFingerprintProvider.EmptyTableSentinel]] rather than crashing or hashing `null`.
  *
  * ==KNOWN GAP, NOT FIXED: the unpinned-read path (resolution step 3) can report a snapshot NEWER
  * than what was actually read==
  * Confirmed empirically, not just reasoned about (see `IcebergFingerprintProviderSpec`'s
  * "KNOWN GAP" test): the same `BatchScanExec`/`SparkTable` OBJECT (verified via `eq`), asked for
  * its fingerprint twice -- once right after the query was planned, once again after the query
  * actually executed, with an unrelated `INSERT` committed in between -- returns a DIFFERENT
  * snapshot id the second time, even though the query's own execution read the FIRST snapshot's
  * data. `SparkTable.table()`'s underlying `Table` handle auto-refreshes on `currentSnapshot()`
  * for an unpinned read; it is not a fixed-at-plan-time snapshot the way the query's own file
  * list is. This matters specifically because the real capture path
  * ([[org.apache.spark.resume.spark35.SparkResumeListener]]) always calls this provider
  * POST-execution: a commit landing in the (usually narrow, but real) window between a query
  * finishing and the listener firing produces an anchor fingerprinted to data NEWER than what was
  * actually captured -- a genuine false-positive-resumption hazard (invariant A-1), not merely an
  * untested edge case.
  *
  * No public API fix was found, and this was verified by exhausting the real candidates, not by
  * assumption: `SparkBatchQueryScan.snapshotId()` -- the field that IS fixed at scan-build time,
  * independent of the table wrapper's live refresh -- is package-private to
  * `org.apache.iceberg.spark.source`; `SparkInputPartition.table()`/`.taskGroup()` (which would
  * give the actual resolved file list, another fixed-at-plan-time source) are public METHODS on a
  * package-private CLASS, unreachable through any public supertype (`InputPartition` declares
  * neither). Both require reflecting into a third-party connector's internals, which this
  * project's own public-API-only posture (docs/DESIGN.md §8) rules out doing silently.
  *
  * Consequence, stated plainly: do not treat an `IcebergFingerprintProvider`-backed anchor as
  * safe against a commit racing the capture listener. This is real production-relevant exposure
  * for the unpinned-read path specifically -- a `VERSION AS OF`-pinned read is unaffected (its
  * `snapshotId()` is fixed on the object, not re-resolved). Mitigating this properly needs
  * resolving the fingerprint BEFORE execution rather than after (a capture-architecture change
  * outside this provider's own scope -- see docs/DESIGN.md §14's Phase 2 Iceberg entry). */
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
