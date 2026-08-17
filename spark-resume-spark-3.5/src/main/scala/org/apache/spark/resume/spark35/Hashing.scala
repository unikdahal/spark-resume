package org.apache.spark.resume.spark35

import java.security.MessageDigest

private[spark35] object Hashing {
  def sha256Hex(s: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
}
