package org.apache.spark.resume.celeborn

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CelebornHandleCodecSpec extends AnyFunSuite with Matchers {

  test("round-trip preserves both fields") {
    val original = CelebornHandle("app-123", 7)
    CelebornHandleCodec.decode(CelebornHandleCodec.encode(original)) shouldBe original
  }

  test("round-trip survives an appUniqueId that itself contains the '|' separator") {
    // The decoder splits on the LAST '|', specifically so this doesn't need escaping.
    val original = CelebornHandle("app-with-a-|-pipe-in-it", 3)
    CelebornHandleCodec.decode(CelebornHandleCodec.encode(original)) shouldBe original
  }

  test("decode rejects a payload this codec did not produce (missing magic tag)") {
    an[IllegalArgumentException] should be thrownBy
      CelebornHandleCodec.decode("not a real payload".getBytes("UTF-8"))
  }

  test("decode rejects a malformed payload with the right tag but no separator") {
    an[IllegalArgumentException] should be thrownBy
      CelebornHandleCodec.decode("CEL1:no-separator-here".getBytes("UTF-8"))
  }

  test("decode rejects a payload whose shuffleId is not a valid Int") {
    an[IllegalArgumentException] should be thrownBy
      CelebornHandleCodec.decode("CEL1:app-1|not-a-number".getBytes("UTF-8"))
  }
}
