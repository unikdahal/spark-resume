# spark-resume-iceberg

An Iceberg `SourceFingerprint` (`docs/DESIGN.md` §14 Phase 2): fingerprints a table read through
Spark's DSv2 path by its resolved Iceberg **snapshot id**, not a file listing — a cheaper, exact
notion of point-in-time identity that Iceberg's own commit model already provides.

A separate module from `spark-resume-spark-3.5` on purpose: `DefaultProviders.all` in that module
deliberately does **not** reference this module's provider, so a user without Iceberg on their
classpath never pulls it in transitively (`NoClassDefFoundError` risk otherwise). Compose it
yourself: `Seq(new IcebergFingerprintProvider(), new FileSourceFingerprint())`.

## What this proves

`IcebergFingerprintProviderSpec` runs real commits against a local Hadoop-catalog Iceberg table
(no external infrastructure needed) and asserts on the actual resulting fingerprints: stability
across two independent `SparkSession`s reading an unmutated table, a changed fingerprint after a
committing `INSERT` (the go/no-go property), discrimination between different tables with
identical content, correct resolution of a `VERSION AS OF` time-travel read (pinned to the
snapshot the query actually asked for, not whatever is current later), and the disclosed
empty-table sentinel for a table with no commits yet. 7 tests, all passing, reproduced clean
across multiple full `mvn clean install` runs.

## How the snapshot id is actually reached — a real finding, not a lookup

Neither `Scan.description()` nor `Scan.toString()` include the snapshot id — verified empirically
(both return the identical string before and after a committing `INSERT`), not assumed from
documentation. The field that *does* hold it, `SparkBatchQueryScan.snapshotId()`, is
package-private to `org.apache.iceberg.spark.source` — unreachable without reflecting into a
third-party connector's internals, which this project's own posture (`docs/DESIGN.md` §8: public
API only, no silent internals-reaching) rules out. The path that *is* public: `BatchScanExec.table`
(Spark's own public DSv2 `Table` accessor) can be cast to `org.apache.iceberg.spark.source.SparkTable`
— a genuinely public Iceberg class, meant to be catalog-visible — whose public
`snapshotId()`/`branch()`/`table()` accessors give everything needed. See
`IcebergFingerprintProvider`'s doc comment for the full three-way resolution order (branch, then
pinned snapshot, then current) and why fingerprinting a branch *name* instead of its resolved tip
would itself be a false-positive-resumption hazard (a branch is a moving pointer).

## What this does NOT prove

Same Tier 1 scope limits as `spark-resume-spark-3.5`: no execution is skipped, this is identity
only. Untested: reading via a named Iceberg branch (the resolution code path exists and is
reasoned about in the doc comment, but no test in this suite commits two snapshots to a branch and
asserts the fingerprint follows the branch's tip rather than staying pinned to the first one).
