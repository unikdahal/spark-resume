package org.apache.spark.resume.fs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FsHandleCodecSpec extends AnyFunSuite with Matchers {

  test("round-trip preserves both fields") {
    val original = FsHandle("slot-123", 7L)
    FsHandleCodec.decode(FsHandleCodec.encode(original)) shouldBe original
  }

  test("round-trip survives a slotId that itself contains the '|' separator") {
    // The decoder splits on the LAST '|', specifically so this doesn't need escaping.
    val original = FsHandle("slot-with-a-|-pipe-in-it", 3L)
    FsHandleCodec.decode(FsHandleCodec.encode(original)) shouldBe original
  }

  test("decode rejects a payload this codec did not produce (missing magic tag)") {
    an[IllegalArgumentException] should be thrownBy
      FsHandleCodec.decode("not a real payload".getBytes("UTF-8"))
  }

  test("decode rejects a malformed payload with the right tag but no separator") {
    an[IllegalArgumentException] should be thrownBy
      FsHandleCodec.decode("FS1:no-separator-here".getBytes("UTF-8"))
  }

  test("decode rejects a payload whose generation is not a valid Long") {
    an[IllegalArgumentException] should be thrownBy
      FsHandleCodec.decode("FS1:slot-1|not-a-number".getBytes("UTF-8"))
  }
}
