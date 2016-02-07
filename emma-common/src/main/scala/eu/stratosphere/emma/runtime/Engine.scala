package eu.stratosphere
package emma.runtime

import emma.api.DataBag
import emma.api.model.Identity
import emma.ir._

import java.util.UUID

/**
 * Parent of all supported runtimes that can process [[DataBag]]s.
 *
 * Extend this to add missing back-ends. If a runtime doesn't support all types of execution
 * (as specified by abstract methods), provide reasonable error messages. Override
 * [[closeSession()]] to shut down the system and clean up resources.
 */
abstract class Engine {

  private var closed = false

  /** A [[Seq]] of plugins to be called at each execution step (initially empty). */
  var plugins: Seq[RuntimePlugin] = Nil

  /** The unique ID of this runtime instance. */
  val sessionId = UUID.randomUUID()

  /** Returns the default degree of parallelism for this runtime. */
  def defaultDoP: Int

  // log program run header
  logger.info("############################################################")
  logger.info("# Emma: Parallel Dataflow Compiler")
  logger.info("############################################################")
  logger.info(s"Starting Emma session $sessionId (${getClass.getSimpleName} runtime)")

  /** Executes a top-level [[Fold]] comprehension. */
  def executeFold[A, B](
    fold: Fold[A, B],
    name: String,
    ctx: Context,
    closure: Any*): A

  /** Materializes temporarily the result of executing a comprehension. */
  def executeTempSink[A](
    sink: TempSink[A],
    name: String,
    ctx: Context,
    closure: Any*): DataBag[A]

  /** Executes a system output task. */
  def executeWrite[A](
    write: Write[A],
    name: String,
    ctx: Context,
    closure: Any*): Unit

  /** Creates a backend for updating [[DataBag]] elements in place. */
  def executeStatefulCreate[A <: Identity[K], K](
    stateful: StatefulCreate[A, K],
    name: String,
    ctx: Context,
    closure: Any*): AbstractStatefulBackend[A, K]

  /** Shuts down the backing runtime and releases all resources. */
  final def close(): Unit =
    if (!closed) {
      closeSession()
      closed = true
    }

  /** Override this method to clean up resources. */
  protected def closeSession(): Unit =
    logger.info(s"Closing Emma session $sessionId (${getClass.getSimpleName} runtime)")
}

object Engine {

  /** Provides methods for convenient runtime initialization. */
  trait Factory {

    /** Returns a runtime instance with default configuration. */
    def default(): Engine

    /** Returns a runtime instance optimized for testing. */
    def testing(): Engine
  }
}
