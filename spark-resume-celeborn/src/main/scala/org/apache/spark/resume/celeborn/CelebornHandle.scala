package org.apache.spark.resume.celeborn

import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.resume.api.ExchangeHandle

/** A [[ExchangeHandle]] identifying one Celeborn-registered shuffle: the application it was
  * registered under (`appUniqueId`, Celeborn's own per-driver-process identity string) and the
  * shuffle id within that application. Both are required to look the shuffle up in Celeborn's
  * master -- a shuffle id alone is not globally unique, only unique within its owning
  * application. */
final case class CelebornHandle(appUniqueId: String, shuffleId: Int) extends ExchangeHandle

/** [[CelebornHandle]]'s wire format -- an explicit, versioned, tagged encoding, not Java
  * serialization, matching the same reasoning `spark-resume-redis`'s `AnchorCodec` documents for
  * why. Tagged with a magic prefix (`CEL1:`) so [[CelebornExchangeStore.deserializeHandle]] can
  * actually tell a foreign payload apart from one of its own, per `ExchangeStore.deserializeHandle`'s
  * contract ("must not silently accept a foreign payload") -- the same real gap
  * `spark-resume-api`'s `InMemoryExchangeStore` was found to have and fixed the same way. */
private[celeborn] object CelebornHandleCodec {

  private val Magic = "CEL1:"

  def encode(handle: CelebornHandle): Array[Byte] =
    s"$Magic${handle.appUniqueId}|${handle.shuffleId}".getBytes(UTF_8)

  def decode(payload: Array[Byte]): CelebornHandle = {
    val text = new String(payload, UTF_8)
    if (!text.startsWith(Magic)) {
      throw new IllegalArgumentException(
        s"payload was not produced by CelebornExchangeStore (missing '$Magic' tag)")
    }
    val body = text.stripPrefix(Magic)
    val lastSep = body.lastIndexOf('|')
    if (lastSep < 0) {
      throw new IllegalArgumentException(s"malformed CelebornHandle payload: $text")
    }
    val appUniqueId = body.substring(0, lastSep)
    val shuffleIdStr = body.substring(lastSep + 1)
    try {
      CelebornHandle(appUniqueId, shuffleIdStr.toInt)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"malformed CelebornHandle payload (bad shuffleId): $text")
    }
  }
}
