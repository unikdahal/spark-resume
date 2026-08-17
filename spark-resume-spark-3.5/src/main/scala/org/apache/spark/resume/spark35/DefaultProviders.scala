package org.apache.spark.resume.spark35

import org.apache.spark.resume.api.SourceFingerprint

/** The `SourceFingerprint` providers this module ships, in priority order. A caller wiring up
  * `spark.sql.catalog`-specific connectors (Iceberg, Delta, ...) supplies their own providers
  * ahead of or instead of this list -- `WholePlanFingerprint.compute` takes the provider sequence
  * as an explicit argument rather than reading it from any global registry, so composing multiple
  * connectors' providers is a caller-side list concatenation, not a plugin-discovery mechanism
  * this module has to own. */
object DefaultProviders {
  val all: Seq[SourceFingerprint] = Seq(new FileSourceFingerprint)
}
