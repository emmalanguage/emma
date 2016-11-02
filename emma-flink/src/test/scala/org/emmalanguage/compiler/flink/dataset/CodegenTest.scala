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

import compiler.BaseCodegenTest

class CodegenTest extends BaseCodegenTest("FlinkDataSet") {

  import compiler._

  lazy val flinkDataSetModuleSymbol = universe.rootMirror.staticModule(s"$rootPkg.api.FlinkDataSet")

  override lazy val backendPipeline: (compiler.u.Tree) => compiler.u.Tree = {
    Comprehension.desugar(API.bagSymbol)
  } andThen {
    Backend.translateToDataflows(flinkDataSetModuleSymbol)
  } andThen {
    addContext
  }

  private lazy val addContext: u.Tree => u.Tree = tree => {
    import u._
    q"""
        implicit val ctx = _root_.org.apache.flink.api.scala.ExecutionEnvironment.getExecutionEnvironment
        $tree
    """
  }
}
