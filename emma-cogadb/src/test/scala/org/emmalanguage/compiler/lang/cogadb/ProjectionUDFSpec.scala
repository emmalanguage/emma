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
package compiler.lang.cogadb

import compiler.RuntimeCompiler
import compiler.BaseCompilerSpec

class ProjectionUDFSpec extends BaseCompilerSpec {

  override val compiler = new RuntimeCompiler with CoGaUDFSupport

  import compiler._
  import u.reify

  lazy val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.anf, collectFirstLambda, CoGaUDFSupport.apply).compose(_.tree)

  lazy val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.anf).compose(_.tree)

  lazy val collectFirstLambda: u.Tree => u.Tree = tree => (tree collect {
    case t: u.Function => t
  }).head

  "simple projection" in {
    val act = testPipeline(reify {
      (x: (Int, Double, String)) => {
        val u: Int = x._1
        u
      }
    })

    val exp = anfPipeline(reify {
      val u = ast.AttrRef("unknown", "_1", "_1", 1)
      u
    })

    act shouldBe alphaEqTo(exp)
  }

  "complex projection" in {
    val act = testPipeline(reify {
      (x: (Int, Double, String)) => {
        val u = x._1
        val v = x._3
        val r = Tuple2.apply(u, v)
        r
      }
    })

    val exp = anfPipeline(reify {
      val u = ast.AttrRef("unknown", "_1", "_1", 1)
      val v = ast.AttrRef("unknown", "_3", "_3", 1)
      val r = Seq.apply(u, v)
      r
    })

    act shouldBe alphaEqTo(exp)
  }
}
