package eu.stratosphere
package emma.runtime

import emma.api.DataBag
import emma.api.model.Identity
import emma.ir._

/** Proxy runtime for native execution on top of Scala collections. */
class Native extends Engine {

  logger.info(s"Initializing Native execution environment")
  sys.addShutdownHook(close())

  override lazy val defaultDoP = 1

  override def executeFold[A, B](
    fold: Fold[A, B],
    name: String,
    ctx: Context,
    closure: Any*): A = ???

  override def executeTempSink[A](
    sink: TempSink[A],
    name: String,
    ctx: Context,
    closure: Any*): DataBag[A] = ???

  override def executeWrite[A](
    write: Write[A],
    name: String,
    ctx: Context,
    closure: Any*): Unit = ???

  override def executeStatefulCreate[A <: Identity[K], K](
    stateful: StatefulCreate[A, K],
    name: String,
    ctx: Context,
    closure: Any*): AbstractStatefulBackend[A, K] = ???
}

object Native extends Engine.Factory {

  def apply(): Native = new Native
  override def default(): Native = apply()
  override def testing(): Native = apply()
}
