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
import emma.codegen.flink.{DataflowGenerator, TempResultsManager}
import emma.codegen.utils.DataflowCompiler
import emma.ir._

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

import scala.reflect.runtime.currentMirror

/**
 * Proxy runtime for Apache Flink.
 *
 * In this runtime [[DataBag]]s are backed by [[DataSet]]s. Comprehensions are compiled to
 * dataflow programs and executed in parallel. Both local and remote environments are supported.
 *
 * @param env The backing Flink execution environment.
 */
class Flink(val env: ExecutionEnvironment) extends Engine {

  logger.info(s"Initializing Flink execution environment")
  sys.addShutdownHook(close())

  private val compiler = new DataflowCompiler(currentMirror)
  private val generator = new DataflowGenerator(compiler, sessionId)
  private val manager = new TempResultsManager(compiler.tb, sessionId)
  override lazy val defaultDoP = env.getParallelism

  def this() =
    this(ExecutionEnvironment.getExecutionEnvironment)

  override def executeFold[A, B](
    fold: Fold[A, B],
    name: String,
    ctx: Context,
    closure: Any*): A = {

    for (p <- plugins) p.handleLogicalPlan(fold, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(fold, name)
    val args = env +: manager +: closure ++: localInputs(fold)
    val result = compiler.execute[A](dataFlow, args.toArray)
    manager.gc()
    result
  }

  override def executeTempSink[A](
    sink: TempSink[A],
    name: String,
    ctx: Context,
    closure: Any*): DataBag[A] = {

    for (p <- plugins) p.handleLogicalPlan(sink, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(sink, name)
    val args = env +: manager +: closure ++: localInputs(sink)
    val dataSet = compiler.execute[DataSet[A]](dataFlow, args.toArray)
    manager.gc()
    DataBag(sink.name, dataSet, dataSet.collect())
  }

  override def executeWrite[A](
    write: Write[A],
    name: String,
    ctx: Context,
    closure: Any*): Unit = {

    for (p <- plugins) p.handleLogicalPlan(write, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(write, name)
    val args = env +: manager +: closure ++: localInputs(write)
    compiler.execute[Unit](dataFlow, args.toArray)
    manager.gc()
  }

  override def executeStatefulCreate[S <: Identity[K], K](
    stateful: StatefulCreate[S, K],
    name: String,
    ctx: Context,
    closure: Any*): AbstractStatefulBackend[S, K] = {

    for (p <- plugins) p.handleLogicalPlan(stateful, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(stateful, name)
    val args = env +: manager +: closure ++: localInputs(stateful)
    val result = compiler.execute[AbstractStatefulBackend[S, K]](dataFlow, args.toArray)
    manager.gc()
    result
  }
}

object Flink extends Engine.Factory {

  def apply(env: ExecutionEnvironment): Flink =
    new Flink(env)

  override def default(): Flink =
    new Flink

  override def testing(): Flink = {
    val conf = new Configuration
    val netBuffers = 4096
    conf.setInteger("taskmanager.network.numberOfBuffers", netBuffers)
    val env = ExecutionEnvironment.createLocalEnvironment(conf)
    env.getConfig.disableSysoutLogging()
    apply(env)
  }
}
