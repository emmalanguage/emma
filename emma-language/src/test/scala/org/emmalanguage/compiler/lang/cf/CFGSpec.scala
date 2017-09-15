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
package compiler.lang.cf

import api._
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import compiler.ir.DSCFAnnotations._
import test.schema.Graphs._

/** A spec for comprehension normalization. */
class CFGSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf
    ).compose(_.tree)

  "control-flow graph" - {
    "with comprehensions" in {
      // input parameters
      val input = "file://path/to/input"
      val output = "file://path/to/output"
      implicit val edgeCSVConverter = CSVConverter[Edge[Long]]

      val tree = idPipeline(reify {
        // read in a directed graph
        val csv$1 = CSV()
        val read$1 = DataBag.readCSV[Edge[Long]](input, csv$1)
        val paths$1 = read$1.distinct
        val count$1 = paths$1.size
        val added$1 = 0L

        @doWhileLoop
        def doWhile$1(added$3: Long, count$3: Long, paths$3: DataBag[Edge[Long]]): Unit = {
          val closure = comprehension[Edge[Long], DataBag] {
            val e1 = generator[Edge[Long], DataBag](paths$3)
            val e2 = generator[Edge[Long], DataBag](paths$3)
            guard {
              val dst$1 = e1.dst
              val src$1 = e2.src
              dst$1 == src$1
            }
            head {
              val src$2 = e1.src
              val dst$2 = e2.dst
              Edge(src$2, dst$2)
            }
          }

          val union$1 = paths$3 union closure
          val paths$2 = union$1.distinct
          val count$2 = paths$2.size
          val added$2 = count$2 - count$3
          val isReady = added$2 > 0

          @suffix def suffix$1(): Unit = {
            paths$2.writeCSV(output, csv$1)
          }

          if (isReady) doWhile$1(added$2, count$2, paths$2)
          else suffix$1()
        }

        doWhile$1(added$1, count$1, paths$1)
      })

      val graph = ControlFlow.cfg(tree)
      val nest = graph.nest.vmap(_.name.toString)
      val ctrl = graph.ctrl.vmap(_.name.toString)
      val data = graph.data.vmap(_.name.toString)

      nest.successors("closure")   should contain allOf (
        "e1", "e2", "src$1", "src$2", "dst$1", "dst$2")
      ctrl.successors("doWhile$1") should contain allOf ("doWhile$1", "suffix$1")
      ctrl.successors("suffix$1")  shouldBe 'empty
      data.predecessors("closure") should not contain "paths$3"                // comprehension
      data.predecessors("closure") should contain allOf ("dst$1", "src$2")     // comprehension
      data.predecessors("e1")      should contain ("paths$3")                  // generator
      data.predecessors("union$1") should contain allOf ("paths$3", "closure") // regular value
      data.predecessors("added$3") should contain allOf ("added$1", "added$2") // method parameter
    }

    "with nested methods" in {
      val tree = anfPipeline(reify {
        implicit val zipSeqWithIdx = Seq.canBuildFrom[(Int, Int)]
        val customers = 4
        val barbers = Seq(10, 5)
        val barber$1 = 0
        val f$1 = (x$1: Int) => {
          val div$1 = 1.0 / x$1
          div$1
        }
        val rate = barbers.map(f$1).sum
        val time$1 = ((0 max (customers - barbers.length - 1)) / rate).toLong - 1
        val less$1 = time$1 < 0
        @elseBranch def else$1(): Int = {
          val f$2 = (x$2: Int) => {
            val div$2 = time$1 / x$2
            val sum$1 = div$2 + 1
            sum$1
          }
          val served$1 = barbers.map(f$2).sum
          suffix$1(served$1)
        }
        @suffix def suffix$1(served$2: Long): Int = {
          val remaining$1 = customers - served$2
          @whileLoop def while$1(barber$2: Int, remaining$2: Long, time$2: Long): Int = {
            val greater$1 = remaining$2 > 0
            @loopBody def body$1(barber$3: Int, remaining$3: Long, time$3: Long): Int = {
              val time$4 = time$3 + barbers.map(b => b - time$3 % b).min
              val iter$1 = barbers.zipWithIndex.toIterator
              val bi$1 = null.asInstanceOf[(Int, Int)]
              @whileLoop def while$2(barber$4: Int, bi$2: (Int, Int), remaining$4: Long): Int = {
                val hasNext$1 = iter$1.hasNext
                @loopBody def body$2(barber$5: Int, bi$3: (Int, Int), remaining$5: Long): Int = {
                  val bi$4 = iter$1.next()
                  val b = bi$4._1
                  val i = bi$4._2
                  val eq$1 = time$4 % b == 0
                  @thenBranch def then$1(): Int = {
                    val remaining$7 = remaining$5 - 1
                    val eq$2 = remaining$7 == 0
                    @thenBranch def then$2(): Int = {
                      val barber$9 = i + 1
                      suffix$2(barber$9)
                    }
                    @suffix def suffix$2(barber$10: Int): Int = {
                      suffix$3(barber$10, remaining$7)
                    }
                    if (eq$2) then$2()
                    else suffix$2(barber$5)
                  }
                  @suffix def suffix$3(barber$11: Int, remaining$8: Long): Int = {
                    while$2(barber$11, bi$4, remaining$8)
                  }
                  if (eq$1) then$1()
                  else suffix$3(barber$5, remaining$5)
                }
                @suffix def suffix$4(barber$12: Int, remaining$9: Long): Int = {
                  while$1(barber$12, remaining$9, time$4)
                }
                if (hasNext$1) body$2(barber$4, bi$2, remaining$4)
                else suffix$4(barber$4, remaining$4)
              }
              while$2(barber$3, bi$1, remaining$3)
            }
            @suffix def suffix$5(barber$13: Int): Int = {
              barber$13
            }
            if (greater$1) body$1(barber$2, remaining$2, time$2)
            else suffix$5(barber$2)
          }
          while$1(barber$1, remaining$1, time$1)
        }
        if (less$1) suffix$1(0L) else else$1()
      })

      val graph = ControlFlow.cfg(tree)
      val nest = graph.nest.vmap(_.name.toString)
      val ctrl = graph.ctrl.vmap(_.name.toString)
      val data = graph.data.vmap(_.name.toString)
      val nestT = nest.tclose
      val ctrlT = ctrl.tclose
      val dataT = data.tclose

      // Method parameters
      val Seq(b13, b12, b11, bs@_*) = for {
        i <- 13 to 1 by -1
        if i < 6 || i > 8
      } yield s"barber$$$i"

      nestT.successors("f$1")   should contain ("div$1")
      nestT.successors("f$2")   should contain allOf ("div$2", "sum$1")
      dataT.predecessors(b13)   should contain allOf (b12, b11, bs: _*)
      dataT.predecessors("f$2") should contain ("barbers")
      // Phi nodes for method parameters
      data.nodes should contain allOf (b12, b11, bs: _*)
      // No data flow for lambda parameters
      data.predecessors("x$1") shouldBe 'empty
      data.predecessors("x$2") shouldBe 'empty
      ctrlT.successors("suffix$1") should contain allOf (
        "while$1", "while$2", (for (i <- 2 to 5) yield s"suffix$$$i"): _*)
    }
  }
}
