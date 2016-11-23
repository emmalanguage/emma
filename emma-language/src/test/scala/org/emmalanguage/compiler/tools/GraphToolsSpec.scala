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
package compiler.tools

import api._
import org.emmalanguage.compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._

import java.nio.file.Paths


class GraphToolsSpec extends BaseCompilerSpec {
  import compiler._
  val anfPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true)(
      Core.anf
    ).compose(_.tree)

  type Edge[T] = (T, T)

  val input = "input"
  val csv = io.csv.CSV()
  val output = "output"

  val coreExpr = anfPipeline(u.reify {
    // read in a directed graph
    val paths$1 = DataBag.readCSV[Edge[Long]](input, csv).distinct
    val count$1 = paths$1.size
    val added$1 = 0L

    def doWhile$1(added$3: Long, count$3: Long, paths$3: DataBag[Edge[Long]]): Unit = {

      val closure = comprehension[Edge[Long], DataBag] {
        val e1 = generator[Edge[Long], DataBag](paths$3)
        val e2 = generator[Edge[Long], DataBag](paths$3)
        guard(e1._2 == e2._1)
        head {
          (e1._1, e2._2)
        }
      }

      val paths$2 = (paths$3 union closure).distinct
      val added$2 = paths$2.size - count$3
      val count$2 = paths$2.size
      val isReady = added$2 > 0

      def suffix$1(): Unit = {
        paths$2.writeCSV(output, csv)
      }

      if (isReady) doWhile$1(added$2, count$2, paths$2)
      else suffix$1()
    }

    doWhile$1(added$1, count$1, paths$1)
  })
  GraphTools.mkJsonGraph("transitiveclosure", coreExpr) shouldBe a [spray.json.JsValue]
}
