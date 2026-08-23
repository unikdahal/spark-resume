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
and proves that explicitly pinned snapshots are stable and discriminating. It also proves that
ordinary, branch, and empty-table reads fail closed: this post-execution integration cannot obtain
their immutable plan-time snapshot through Iceberg's public API.

## How the snapshot id is actually reached — a real finding, not a lookup

Neither `Scan.description()` nor `Scan.toString()` include the snapshot id — verified empirically
(both return the identical string before and after a committing `INSERT`), not assumed from
documentation. The field that *does* hold it, `SparkBatchQueryScan.snapshotId()`, is
package-private to `org.apache.iceberg.spark.source` — unreachable without reflecting into a
third-party connector's internals, which this project's own posture (`docs/DESIGN.md` §8: public
API only, no silent internals-reaching) rules out. The path that *is* public: `BatchScanExec.table`
(Spark's own public DSv2 `Table` accessor) can be cast to `org.apache.iceberg.spark.source.SparkTable`
— a genuinely public Iceberg class, meant to be catalog-visible — whose public
`snapshotId()` accessor gives an immutable identity for explicitly pinned reads. `branch()` and
`table().currentSnapshot()` are moving references and are deliberately not used for admission.

## What this does NOT prove

Same Tier 1 scope limits as `spark-resume-spark-3.5`: no execution is skipped, this is identity
only. Ordinary and named-branch reads are not resumable until Spark or Iceberg exposes their
immutable resolved snapshot before execution.

## Unpinned reads fail closed

Confirmed by a dedicated test (`IcebergFingerprintProviderSpec`'s "KNOWN GAP" case), not just
suspected: the SAME `BatchScanExec`/`SparkTable` object, fingerprinted once right after a query is
planned and again after it actually executes (with an unrelated `INSERT` committed in between),
returns a different snapshot id the second time even though the query read the first snapshot.
The provider now rejects that shape, and `WholePlanFingerprint` converts the rejection into a
unique `NON_REUSABLE` token. Consequently two computations cannot match and admission recomputes.
This closes the wrong-answer exposure; it does not yet provide resumption for ordinary reads.
