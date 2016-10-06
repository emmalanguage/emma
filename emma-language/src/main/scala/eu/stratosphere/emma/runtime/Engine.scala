/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
