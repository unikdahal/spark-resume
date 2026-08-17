package org.apache.spark.resume.redis

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.resume.api.Anchor

/** [[Anchor]]'s wire format for [[RedisAnchorStore]] -- an explicit, versioned encoding, not Java
  * serialization. Java serialization was considered and rejected: it is not a compatibility
  * commitment (its wire format is a JVM implementation detail, tied to `serialVersionUID`/class
  * shape, and unreadable from any non-JVM tooling that might ever want to inspect anchors stored
  * in Redis directly), where `Anchor.schemaVersion` already exists specifically to be that
  * commitment. `encode` writes it FIRST and `decode` dispatches on it, so a future format change
  * is additive (a new `case` here), not a breaking rewrite of every stored anchor.
  *
  * Length-prefixed, big-endian, via `DataOutputStream`/`DataInputStream` -- boring and auditable
  * on purpose; no serialization library dependency for a handful of fixed fields. */
private[redis] object AnchorCodec {

  private val SchemaV1 = "1"

  def encode(anchor: Anchor): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    out.writeUTF(SchemaV1)
    out.writeUTF(anchor.queryId)
    out.writeLong(anchor.generation)
    out.writeUTF(anchor.fingerprint)
    out.writeUTF(anchor.handleKind)
    out.writeInt(anchor.handlePayload.length)
    out.write(anchor.handlePayload)
    out.writeInt(anchor.numMappers)
    out.writeInt(anchor.numPartitions)
    out.writeInt(anchor.bytesByPartition.length)
    anchor.bytesByPartition.foreach(out.writeLong)
    out.writeLong(anchor.rowCount)
    out.writeLong(anchor.createdAtMs)
    out.flush()
    bos.toByteArray
  }

  def decode(bytes: Array[Byte]): Anchor = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    val schemaVersion = in.readUTF()
    schemaVersion match {
      case SchemaV1 =>
        val queryId = in.readUTF()
        val generation = in.readLong()
        val fingerprint = in.readUTF()
        val handleKind = in.readUTF()
        val payloadLen = in.readInt()
        val handlePayload = new Array[Byte](payloadLen)
        in.readFully(handlePayload)
        val numMappers = in.readInt()
        val numPartitions = in.readInt()
        val bytesLen = in.readInt()
        val bytesByPartition = new Array[Long](bytesLen)
        var i = 0
        while (i < bytesLen) { bytesByPartition(i) = in.readLong(); i += 1 }
        val rowCount = in.readLong()
        val createdAtMs = in.readLong()
        Anchor(schemaVersion, queryId, generation, fingerprint, handleKind, handlePayload,
          numMappers, numPartitions, bytesByPartition, rowCount, createdAtMs)
      case other =>
        throw new IllegalArgumentException(s"AnchorCodec: unsupported schemaVersion '$other'")
    }
  }
}
