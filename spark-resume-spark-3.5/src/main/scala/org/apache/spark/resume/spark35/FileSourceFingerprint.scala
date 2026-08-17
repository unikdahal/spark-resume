package org.apache.spark.resume.spark35

import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types.StructType

import org.apache.spark.resume.api.{FingerprintTarget, SourceFingerprint}

/** [[SourceFingerprint]] for the built-in DSv1 file-source scan (`FileSourceScanExec`) -- Spark's
  * own path/Parquet/CSV/JSON/ORC file reader, covering plain local and object-store-backed
  * tables read without a DSv2 connector in front of them.
  *
  * The fingerprint is the sorted, hashed list of every file actually read -- path, length,
  * modification time, and resolved partition values -- NOT the query text and NOT the table
  * name alone: two runs of the identical query against a table whose underlying files changed
  * between runs (a partition overwritten, a new file landed) MUST produce different fingerprints
  * (design invariant A-1) -- this is the property the negative conformance test in this module's
  * test suite exists to check, not just assert.
  *
  * Dynamic-pruning filters (the ones a runtime subquery injects, e.g. broadcast-join partition
  * pruning) are excluded from the file listing's filter set before listing: a dynamic filter's
  * own predicate value can differ across runs of the identical query (different broadcast
  * values), and letting it influence which files get listed would make the fingerprint unstable
  * for reasons that have nothing to do with the actual data read -- see the stability test. */
final class FileSourceFingerprint extends SourceFingerprint {

  override def supports(target: FingerprintTarget): Boolean =
    target.node.isInstanceOf[FileSourceScanExec]

  override def fingerprint(target: FingerprintTarget): String = {
    val scan = target.node.asInstanceOf[FileSourceScanExec]
    val partitionSchema: StructType = scan.relation.location.partitionSchema
    val staticFilters = scan.partitionFilters.filterNot(isDynamicPruningFilter)
    val dirs = scan.relation.location.listFiles(staticFilters, scan.dataFilters)
    val entries = dirs.flatMap { pd =>
      val partVals = partitionValuesString(pd.values, partitionSchema)
      pd.files.map(f => s"${f.getPath}|${f.getLen}|${f.getModificationTime}|$partVals")
    }
    // Sorted: file-listing order is not a property of the data, and must not leak into the
    // fingerprint -- two listings of the identical file set in different orders must collide.
    Hashing.sha256Hex(entries.sorted.mkString(";"))
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  private def partitionValuesString(row: org.apache.spark.sql.catalyst.InternalRow,
                                     schema: StructType): String =
    schema.fields.zipWithIndex.map { case (f, i) =>
      if (row.isNullAt(i)) s"${f.name}=NULL" else s"${f.name}=${row.get(i, f.dataType)}"
    }.mkString(",")

}
