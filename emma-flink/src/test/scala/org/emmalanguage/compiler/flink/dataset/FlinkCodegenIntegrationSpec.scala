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
package org.emmalanguage
package compiler.flink.dataset

import api._
import api.flink._
import compiler.BaseCodegenIntegrationSpec
import compiler.FlinkCompiler
import compiler.RuntimeCompiler

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment

class FlinkCodegenIntegrationSpec extends BaseCodegenIntegrationSpec {

  override lazy val compiler = new RuntimeCompiler with FlinkCompiler

  import compiler._

  lazy val flinkDataSet = api.Sym[FlinkDataSet.type].asModule
  lazy val flinkMutableBag = api.Sym[FlinkMutableBag.type].asModule

  override lazy val backendPipeline: u.Tree => u.Tree =
    Function.chain(Seq(
      Comprehension.desugar(API.DataBag.sym),
      Backend.specialize(FlinkAPI),
      addContext
    ))

  override val idPipeline: u.Expr[Any] => u.Tree = {
    (_: u.Expr[Any]).tree
  } andThen {
    compiler.pipeline(typeCheck = true)(addContext)
  } andThen {
    checkCompile
  }

  private lazy val addContext: u.Tree => u.Tree = tree => {
    import u._
    q"""
        implicit val ctx = _root_.org.apache.flink.api.scala.ExecutionEnvironment.getExecutionEnvironment
        $tree
    """
  }

  implicit val ctx = ExecutionEnvironment.getExecutionEnvironment

  // --------------------------------------------------------------------------
  // Distributed collection conversion
  // --------------------------------------------------------------------------

  "Convert from/to a Flink DataSet" in verify(u.reify {
    val xs = DataBag(1 to 1000).withFilter(_ > 800)
    val ys = xs.as[DataSet].filter(_ < 200)
    val zs = DataBag.from(ys)
    zs.size
  })
}
