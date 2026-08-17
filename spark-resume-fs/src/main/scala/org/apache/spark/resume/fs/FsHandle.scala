package org.apache.spark.resume.fs

import java.nio.charset.StandardCharsets.UTF_8

/** A slot identity plus the generation it was written at, on disk under
  * [[FsExchangeStore]]'s own `baseDir` (deliberately not part of the handle -- a deployment-level
  * config, same as `CelebornExchangeStore`'s `masterRestBaseUrl` constructor arg, not something a
  * portable handle should embed). `generation` is what `isFresh` compares against the slot's
  * current-generation marker (see `FsExchangeStore`'s doc comment for the atomic-rename mechanism
  * that maintains it). */
final case class FsHandle(slotId: String, generation: Long) extends org.apache.spark.resume.api.ExchangeHandle

/** Same wire-format shape as `spark-resume-celeborn`'s `CelebornHandleCodec` (magic prefix +
  * `|`-delimited fields, `lastIndexOf` so a `slotId` containing `|` still round-trips) -- proven
  * pattern, reused deliberately rather than re-derived. */
private[fs] object FsHandleCodec {
  private val Magic = "FS1:"

  def encode(handle: FsHandle): Array[Byte] =
    s"$Magic${handle.slotId}|${handle.generation}".getBytes(UTF_8)

  def decode(payload: Array[Byte]): FsHandle = {
    val text = new String(payload, UTF_8)
    if (!text.startsWith(Magic)) {
      throw new IllegalArgumentException(s"FsHandleCodec: payload does not start with magic '$Magic'")
    }
    val body = text.stripPrefix(Magic)
    val lastSep = body.lastIndexOf('|')
    if (lastSep < 0) {
      throw new IllegalArgumentException(s"FsHandleCodec: no '|' separator found in payload body '$body'")
    }
    val slotId = body.substring(0, lastSep)
    val generationStr = body.substring(lastSep + 1)
    try {
      FsHandle(slotId, generationStr.toLong)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"FsHandleCodec: '$generationStr' is not a valid Long generation")
    }
  }
}
