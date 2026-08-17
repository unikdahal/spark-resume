package org.apache.spark.resume.spark35

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/** Encodes/decodes one partition's rows to/from a single, self-contained byte array -- the actual
  * "shuffle bytes" a real `ExchangeStore.store`/`readPartition` round-trips for real execution-
  * skipping (see `ExecutionSkipRule`/`SkippedShuffleRDD`). Uses Spark's OWN `UnsafeRow` binary
  * representation directly, length-prefixed per row -- the same shape Spark's own shuffle write
  * path (`UnsafeRowSerializer`) uses internally, not a bespoke format invented for this project.
  *
  * `numFields` (not the full schema) is all `decode` needs to reconstruct a row: `UnsafeRow` is a
  * fixed-layout binary format over a raw byte buffer where field offsets are computed from the
  * field COUNT alone (the null-tracking bitmap's size is a function of `numFields`) -- the actual
  * field TYPES are never needed to read the bytes back, only to interpret them afterward, which is
  * the caller's concern (the same reason `ExchangeStore.readPartition` returns opaque
  * `Array[Byte]`: this codec, and the SPI it serves, has no schema of its own to carry).
  *
  * Deliberately narrower than "encode any `InternalRow`": rows reaching this codec are expected to
  * already be `UnsafeRow` (true whole-stage-codegen output, which is what a materialized
  * `Exchange`'s child produces in practice) -- a disclosed scope limit, not silently worked around
  * by a slower generic path, so a row shape this codec doesn't support fails loudly instead of
  * corrupting data silently. */
object RowBytesCodec {

  def encode(rows: Iterator[InternalRow]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new DataOutputStream(baos)
    rows.foreach {
      case unsafe: UnsafeRow =>
        out.writeInt(unsafe.getSizeInBytes)
        out.write(unsafe.getBytes)
      case other =>
        throw new IllegalArgumentException(
          "RowBytesCodec.encode: expected UnsafeRow (real whole-stage-codegen output), got " +
            s"${other.getClass.getName} -- this codec's scope is disclosed as UnsafeRow-only, " +
            "not silently degraded to a slower generic path for anything else.")
    }
    out.flush()
    baos.toByteArray
  }

  def decode(bytes: Array[Byte], numFields: Int): Iterator[InternalRow] = new Iterator[InternalRow] {
    private val in = new DataInputStream(new ByteArrayInputStream(bytes))
    private var nextRow: UnsafeRow = readNext()

    private def readNext(): UnsafeRow = {
      if (in.available() <= 0) {
        null
      } else {
        val size = in.readInt()
        val buf = new Array[Byte](size)
        in.readFully(buf)
        val row = new UnsafeRow(numFields)
        row.pointTo(buf, size)
        row
      }
    }

    override def hasNext: Boolean = nextRow != null

    override def next(): InternalRow = {
      if (nextRow == null) throw new NoSuchElementException("RowBytesCodec: no more rows")
      val r = nextRow
      nextRow = readNext()
      r
    }
  }

  /** How many rows `encode` wrote into `bytes` -- schema/`numFields`-AGNOSTIC, deliberately: only
    * scans the length-prefixes and skips forward, never constructs an `UnsafeRow` at all. Exists
    * specifically for a caller (`StageCaptureListener`'s real-bytes capture path) that needs a
    * real row count but does not have a field count on hand at that point -- calling [[decode]]
    * with a wrong/guessed `numFields` just to count rows would be architecturally sloppy (an
    * `UnsafeRow` built with the wrong field count is a real, if latent, correctness hazard for any
    * FUTURE caller who also tries to read its fields), not merely inefficient. */
  def countRows(bytes: Array[Byte]): Long = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    var count = 0L
    while (in.available() > 0) {
      val size = in.readInt()
      in.skipBytes(size)
      count += 1
    }
    count
  }
}
