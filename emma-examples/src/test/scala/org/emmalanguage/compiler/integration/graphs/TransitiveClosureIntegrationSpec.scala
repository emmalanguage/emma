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
package compiler.integration.graphs

import api._
import compiler.integration.BaseCompilerIntegrationSpec
import compiler.ir.ComprehensionSyntax._
import compiler.ir.DSCFAnnotations._
import lib.graphs._

class TransitiveClosureIntegrationSpec extends BaseCompilerIntegrationSpec {

  import compiler._
  import u.reify

  // ---------------------------------------------------------------------------
  // Program closure
  // ---------------------------------------------------------------------------

  // input parameters
  val input: String = null
  val output: String = null
  val csv = CSV()
  implicit val edgeCSVConverter = CSVConverter[Edge[Int]]

  // ---------------------------------------------------------------------------
  // Program representations
  // ---------------------------------------------------------------------------

  val sourceExpr = liftPipeline(reify {
    // read an initial collection of edges
    val edges = DataBag.readCSV[Edge[Int]](input, csv)
    // compute the transitive closure of the edges
    val closure = transitiveClosure(edges)
    // write the results into a CSV file
    closure.writeCSV(output, csv)
  })

  val coreExpr = anfPipeline(reify {
    // read in a directed graph
    val input: this.input.type = this.input
    val csv$1: this.csv.type   = this.csv
    val readCSV = DataBag.readCSV[Edge[Int]](input, csv$1)
    val paths$1 = readCSV.distinct
    val count$1 = paths$1.size
    @whileLoop def doWhile$1(added$3: Long, count$3: Long, paths$3: DataBag[Edge[Int]]): Unit = {
      val closure = comprehension[Edge[Int], DataBag] {
        val e1 = generator[Edge[Int], DataBag](paths$3)
        val e2 = generator[Edge[Int], DataBag](paths$3)
        guard(e1.dst == e2.src)
        head {
          Edge(e1.src, e2.dst)
        }
      }

      val paths$2 = (paths$3 union closure).distinct
      val added$2 = paths$2.size - count$3
      val count$2 = paths$2.size
      val isReady = added$2 > 0
      @suffix def suffix$1(): Unit = {
        val output: this.output.type = this.output
        val csv$2:  this.csv.type    = this.csv
        paths$2.writeCSV(output, csv$2)
      }

      if (isReady) doWhile$1(added$2, count$2, paths$2)
      else suffix$1()
    }

    doWhile$1(0L, count$1, paths$1)
  })

  // ---------------------------------------------------------------------------
  // Specs
  // ---------------------------------------------------------------------------

  "lifting" in {
    sourceExpr shouldBe alphaEqTo(coreExpr)
  }
}
