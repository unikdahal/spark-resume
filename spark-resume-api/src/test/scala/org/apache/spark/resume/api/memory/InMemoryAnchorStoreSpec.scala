package org.apache.spark.resume.api.memory

import org.apache.spark.resume.api.AnchorStore
import org.apache.spark.resume.api.testkit.AnchorStoreContract

class InMemoryAnchorStoreSpec extends AnchorStoreContract {
  override def newStore(): AnchorStore = new InMemoryAnchorStore
}
