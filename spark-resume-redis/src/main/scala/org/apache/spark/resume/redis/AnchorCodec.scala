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
    // Written from the CALLER's own schemaVersion, not a hardcoded constant -- an earlier version
    // of this method wrote SchemaV1 unconditionally regardless of what anchor.schemaVersion
    // actually said, which would have silently rewritten an anchor's own declared compatibility
    // commitment on every encode. Rejected here (fail-closed, A-2) rather than encoded and left
    // for `decode` to discover later: this codec only knows how to WRITE the v1 layout below, so
    // an anchor claiming any other version must not be encoded as if it were v1.
    if (anchor.schemaVersion != SchemaV1) {
      throw new IllegalArgumentException(
        s"AnchorCodec: cannot encode schemaVersion '${anchor.schemaVersion}' (only '$SchemaV1' is supported)")
    }
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    out.writeUTF(anchor.schemaVersion)
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
