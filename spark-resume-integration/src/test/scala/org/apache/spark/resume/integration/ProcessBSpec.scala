package org.apache.spark.resume.integration

import org.scalatest.funsuite.AnyFunSuite

/** Runs [[ProcessB]] as its own forked JVM -- see [[ProcessASpec]]'s doc comment for why this is
  * a separate `mvn test -Dsuites=...` invocation, not a suite bundled alongside `ProcessASpec`
  * (bundling both in one `mvn test` run would put them in the SAME JVM, defeating the entire
  * point of this module: proving the pipeline composes across a REAL process boundary, not just
  * across two objects in one process). `run-integration-test.sh` always runs `ProcessASpec` to
  * completion, in its own `mvn` invocation, before starting `ProcessBSpec`'s. */
class ProcessBSpec extends AnyFunSuite {
  test("ProcessB: cross-process admit, then SafeReattach.attempt -> RefusedUnsupported") {
    ProcessB.main(Array.empty)
  }
}
