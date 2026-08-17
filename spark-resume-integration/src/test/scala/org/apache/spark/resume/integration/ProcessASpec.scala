package org.apache.spark.resume.integration

import org.scalatest.funsuite.AnyFunSuite

/** Runs [[ProcessA]] as its own forked JVM (via `mvn test -Dsuites=...ProcessASpec`, invoked as
  * its own separate `mvn` process by `run-integration-test.sh`, NOT together with
  * `ProcessBSpec` in the same `mvn test` run) -- reuses `scalatest-maven-plugin`'s already-proven
  * forking (correct JDK 17 `--add-opens` set, correct classpath resolution) instead of hand-
  * rolling a `java -cp` invocation, which is exactly what this module tried first and hit
  * unexplained JPMS `IllegalAccessError`s from that inconsistent with every other real Spark
  * session in this repo -- not worth chasing further when this project's own existing,
  * already-proven test-forking infrastructure does the same job for free. See README.md. */
class ProcessASpec extends AnyFunSuite {
  test("ProcessA: produce a real anchor with a real CelebornHandle") {
    ProcessA.main(Array.empty)
  }
}
