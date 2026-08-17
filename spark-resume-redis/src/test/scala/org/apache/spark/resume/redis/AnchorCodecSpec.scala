package org.apache.spark.resume.redis

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.resume.api.Anchor

/** [[Anchor]] is a case class with `Array[Byte]`/`Array[Long]` fields, so its generated `equals`
  * is REFERENCE equality on those two fields, not structural -- `decode(encode(a)) == a` would
  * silently pass for the wrong reason (or fail for the wrong reason) if compared with `shouldBe`
  * directly, since a round-tripped anchor is never the same array instance. This suite compares
  * every field explicitly instead, so a real content mismatch (not an object-identity mismatch)
  * is what actually gets caught. */
class AnchorCodecSpec extends AnyFunSuite with Matchers {

  private def sampleAnchor: Anchor = Anchor(
    schemaVersion = "1",
    queryId = "q-codec",
    generation = 42L,
    fingerprint = "abc123",
    handleKind = "celeborn",
    handlePayload = Array[Byte](1, 2, 3, 4, 5),
    numMappers = 7,
    numPartitions = 4,
    bytesByPartition = Array(100L, 200L, 0L, 400L),
    rowCount = 999L,
    createdAtMs = 1234567890123L)

  test("round-trip preserves every field, compared by CONTENT not by reference") {
    val original = sampleAnchor
    val decoded = AnchorCodec.decode(AnchorCodec.encode(original))

    decoded.schemaVersion shouldBe original.schemaVersion
    decoded.queryId shouldBe original.queryId
    decoded.generation shouldBe original.generation
    decoded.fingerprint shouldBe original.fingerprint
    decoded.handleKind shouldBe original.handleKind
    decoded.handlePayload shouldBe original.handlePayload // Matchers' shouldBe on Array IS content-aware
    decoded.numMappers shouldBe original.numMappers
    decoded.numPartitions shouldBe original.numPartitions
    decoded.bytesByPartition shouldBe original.bytesByPartition
    decoded.rowCount shouldBe original.rowCount
    decoded.createdAtMs shouldBe original.createdAtMs
  }

  test("round-trip with EMPTY arrays (zero mappers/partitions recorded) does not crash or misparse") {
    val original = sampleAnchor.copy(handlePayload = Array.emptyByteArray, bytesByPartition = Array.empty[Long])
    val decoded = AnchorCodec.decode(AnchorCodec.encode(original))
    decoded.handlePayload shouldBe empty
    decoded.bytesByPartition shouldBe empty
  }

  test("encode WRITES the caller's own schemaVersion rather than silently overwriting it") {
    // Guards against a real bug this exact test caught: an earlier version of encode() wrote a
    // hardcoded "1" regardless of what the Anchor itself declared, silently rewriting its own
    // compatibility commitment. sampleAnchor already uses "1" so this couldn't have caught it --
    // this test exists specifically because that one didn't.
    val encoded = AnchorCodec.encode(sampleAnchor)
    AnchorCodec.decode(encoded).schemaVersion shouldBe "1"
  }

  test("encode REJECTS an anchor whose schemaVersion this codec doesn't know how to write") {
    an[IllegalArgumentException] should be thrownBy AnchorCodec.encode(sampleAnchor.copy(schemaVersion = "2"))
  }

  test("decode rejects an unrecognized schemaVersion rather than misparsing later fields") {
    // A hand-built payload declaring a schema version this codec doesn't know, so decode must
    // refuse it outright instead of reading garbage as if it were v1's field layout.
    val bos = new java.io.ByteArrayOutputStream()
    val out = new java.io.DataOutputStream(bos)
    out.writeUTF("99-from-the-future")
    out.writeUTF("whatever-bytes-follow")
    out.flush()
    an[IllegalArgumentException] should be thrownBy AnchorCodec.decode(bos.toByteArray)
  }
}
