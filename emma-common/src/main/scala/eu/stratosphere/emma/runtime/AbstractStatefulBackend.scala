package eu.stratosphere.emma.runtime

import java.util.UUID

trait AbstractStatefulBackend[S, K] {

  private val STATEFUL_BASE_PATH = String.format("%s/emma/stateful", System.getProperty("java.io.tmpdir"))

  private val uuid = UUID.randomUUID()
  private val fileNameBase = s"$STATEFUL_BASE_PATH/$uuid"
  protected var seqNumber = 0

  protected def currentFileName() = s"${fileNameBase}_$seqNumber"
  protected def oldFileName() = s"${fileNameBase}_${seqNumber - 1}"

}
