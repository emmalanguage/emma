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
package compiler.integration

import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for comprehension normalization. */
@RunWith(classOf[JUnitRunner])
class TransitiveClosureSpec extends BaseCompilerSpec {

  import TransitiveClosureSpec._
  import eu.stratosphere.emma.api._

  import compiler._

  // ---------------------------------------------------------------------------
  // Transformation pipelines
  // ---------------------------------------------------------------------------

  val anfPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true)(
      Core.anf
    ).compose(_.tree)

  val liftPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true)(
      Core.lift
    ).compose(_.tree)

  // ---------------------------------------------------------------------------
  // Program closure
  // ---------------------------------------------------------------------------

  // input parameters
  val input = "file://path/to/input"
  val output = "file://path/to/output"
  // implicit parameters
  implicit val csvConverters = implicitly[CSVConverters[Edge[Long]]]

  // ---------------------------------------------------------------------------
  // Program representations
  // ---------------------------------------------------------------------------

  val sourceExpr = liftPipeline(u.reify {
    // read in a directed graph
    var paths = read(input, new CSVInputFormat[Edge[Long]]).distinct()
    var count = paths.size
    var added = 0L

    do {
      val delta = for {
        e1 <- paths
        e2 <- paths
        if e1.dst == e2.src
      } yield Edge(e1.src, e2.dst)

      paths = (paths plus delta).distinct()

      added = paths.size - count
      count = paths.size
    } while (added > 0)

    write(output, new CSVOutputFormat[Edge[Long]])(paths)
  })

  val coreExpr = anfPipeline(u.reify {
    // read in a directed graph
    val paths$1 = read(input, new CSVInputFormat[Edge[Long]]).distinct()
    val count$1 = paths$1.size
    val added$1 = 0L

    def doWhile$1(added$3: Long, count$3: Long, paths$3: DataBag[Edge[Long]]): Unit = {

      val closure = comprehension[Edge[Long], DataBag] {
        val e1 = generator[Edge[Long], DataBag](paths$3)
        val e2 = generator[Edge[Long], DataBag](paths$3)
        guard(e1.dst == e2.src)
        head {
          Edge(e1.src, e2.dst)
        }
      }

      val paths$2 = (paths$3 plus closure).distinct()
      val added$2 = paths$2.size - count$3
      val count$2 = paths$2.size
      val isReady = added$2 > 0

      def suffix$1(): Unit = {
        write(output, new CSVOutputFormat()(csvConverters))(paths$2)
      }

      if (isReady) doWhile$1(added$2, count$2, paths$2)
      else suffix$1()
    }

    doWhile$1(added$1, count$1, paths$1)
  })

  // ---------------------------------------------------------------------------
  // Specs
  // ---------------------------------------------------------------------------

  "lifting" in {
    sourceExpr shouldBe alphaEqTo(coreExpr)
  }
}

object TransitiveClosureSpec {

  import api.model._

  case class Edge[VT](@id src: VT, @id dst: VT) extends Identity[Edge[VT]] {
    def identity = Edge(src, dst)
  }

}
