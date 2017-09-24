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
import examples.graphs._
import examples.graphs.model._

import scala.Ordering.Implicits._

/** A spec for comprehension normalization. */
class EnumerateTrianglesIntegrationSpec extends BaseCompilerIntegrationSpec {

  import compiler._
  import u.reify

  // ---------------------------------------------------------------------------
  // Program closure
  // ---------------------------------------------------------------------------

  // input parameters
  val input: String = null
  val csv = CSV()
  implicit val edgeCSVConverter = CSVConverter[Edge[Long]]

  // ---------------------------------------------------------------------------
  // Program representations
  // ---------------------------------------------------------------------------

  val sourceExpr = liftPipeline(reify {
    // convert a list of directed edges
    val incoming = DataBag.readCSV[Edge[Long]](input, csv)
    val outgoing = incoming.map(e => Edge(e.dst, e.src))
    val edges = (incoming union outgoing).distinct
    // compute all triangles
    val triangles = EnumerateTriangles[Long](edges)
    // count the number of enumerated triangles
    val triangleCount = triangles.size
    // print the result to the console
    println(s"The number of triangles in the graph is $triangleCount")
  })

  val coreExpr = anfPipeline(reify {
    // convert a list of directed edges
    val input: this.input.type = this.input
    val csv:   this.csv.type   = this.csv
    val incoming = DataBag.readCSV[Edge[Long]](input, csv)
    val outgoing = comprehension[Edge[Long], DataBag] {
      val e = generator[Edge[Long], DataBag](incoming)
      head(Edge(e.dst, e.src))
    }
    val edges$r1 = (incoming union outgoing).distinct
    val triangles = comprehension[Triangle[Long], DataBag] {
      val e1 = generator[Edge[Long], DataBag](edges$r1)
      val e2 = generator[Edge[Long], DataBag](edges$r1)
      val e3 = generator[Edge[Long], DataBag](edges$r1)
      guard {
        val x$1 = e1.src
        val u$1 = e1.dst
        val ops$1 = infixOrderingOps[Long](x$1)(scala.math.Ordering.Long)
        ops$1 < u$1
      }
      guard {
        val y$1 = e2.src
        val v$1 = e2.dst
        val ops$1 = infixOrderingOps[Long](y$1)(scala.math.Ordering.Long)
        ops$1 < v$1
      }
      guard {
        val z$1 = e3.src
        val w$1 = e3.dst
        val ops$1 = infixOrderingOps[Long](z$1)(scala.math.Ordering.Long)
        ops$1 < w$1
      }
      guard {
        val u$2 = e1.dst
        val y$2 = e2.src
        u$2 == y$2
      }
      guard {
        val x$2 = e1.src
        val z$2 = e3.src
        x$2 == z$2
      }
      guard {
        val v$2 = e2.dst
        val w$2 = e3.dst
        v$2 == w$2
      }
      head {
        val x$3 = e1.src
        val u$3 = e1.dst
        val v$3 = e2.dst
        Triangle(x$3, u$3, v$3)
      }
    }
    // count the number of enumerated triangles
    val triangleCount = triangles.size
    // print the result to the console
    println(s"The number of triangles in the graph is $triangleCount")
  })

  // ---------------------------------------------------------------------------
  // Specs
  // ---------------------------------------------------------------------------

  "lifting" in {
    sourceExpr shouldBe alphaEqTo(coreExpr)
  }
}
