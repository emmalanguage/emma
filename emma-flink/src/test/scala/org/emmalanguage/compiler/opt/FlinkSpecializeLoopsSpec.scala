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
package compiler.opt

import api._
import api.flink.FlinkNtv
import compiler.BaseCompilerSpec
import compiler.FlinkCompiler
import compiler.RuntimeCompiler
import compiler.ir.ComprehensionSyntax._
import compiler.ir.DSCFAnnotations._
import test.schema.Graphs._

import java.util.UUID

class FlinkSpecializeLoopsSpec extends BaseCompilerSpec with FlinkAware {

  override val compiler = new RuntimeCompiler with FlinkCompiler

  import compiler._
  import u.reify

  lazy val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(
      Core.lift,
      FlinkSpecializeLoops.specializeLoops.timed
    ).compose(_.tree)

  lazy val dscfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(
      Core.lift
    ).compose(_.tree)

  protected override def wrapInClass(tree: u.Tree): u.Tree = {
    import u.Quasiquote

    val Cls = api.TypeName(UUID.randomUUID().toString)
    val run = api.TermName(RuntimeCompiler.default.runMethod)
    val prs = api.Tree.closure(tree).map { sym =>
      val x = sym.name
      val T = sym.info
      q"val $x: $T"
    }

    q"""
    class $Cls {
      def $run(..$prs)(implicit flink: ${FlinkAPI.ExecutionEnvironment}) = $tree
    }
    """
  }

  type Seq3 = Edge[Int]

  val edges = for (c <- 'a' to 'y') yield Edge(c.toInt, c.toInt + 1)
  val N = 5

  "desugared for-loop #1" in withDefaultFlinkEnv(implicit flink => {
    val act = testPipeline(reify {
      val N = this.N
      val edges = this.edges
      val paths = FlinkDataSet(edges)

      val from = Predef.intWrapper(0)
      val to = from.until(N)
      val it = to.toIterator

      val i$1 = null.asInstanceOf[Int]
      @whileLoop def while$1(i: Int, paths: DataBag[Edge[Int]]): DataBag[Edge[Int]] = {
        val hasNext = it.hasNext

        @loopBody def body$1(): DataBag[Edge[Int]] = {
          val next = it.next()
          val delta = comprehension[Edge[Int], DataBag]({
            val e1 = generator[Edge[Int], DataBag]({
              paths
            })
            val e2 = generator[Edge[Int], DataBag]({
              paths
            })
            guard({
              val dst = e1.dst
              val src = e2.src
              val eqs = dst == src
              eqs
            })
            head[Edge[Int]]({
              val src = e1.src
              val dst = e2.dst
              val edg = Edge(src, dst)
              edg
            })
          })
          val union = paths union delta
          val distinct = union.distinct
          while$1(next, distinct)
        }

        @suffix def suffix$1(): DataBag[Edge[Int]] = {
          paths
        }

        if (hasNext) body$1()
        else suffix$1()
      }

      while$1(i$1, paths)
    })

    val exp = dscfPipeline(reify {
      val N = this.N
      val edges = this.edges
      val paths = FlinkDataSet(edges)

      val reslt = FlinkNtv.iterate(paths)(N, paths => {
        val delta = comprehension[Edge[Int], DataBag]({
          val e1 = generator[Edge[Int], DataBag]({
            paths
          })
          val e2 = generator[Edge[Int], DataBag]({
            paths
          })
          guard({
            val dst = e1.dst
            val src = e2.src
            val eqs = dst == src
            eqs
          })
          head[Edge[Int]]({
            val src = e1.src
            val dst = e2.dst
            val edg = Edge(src, dst)
            edg
          })
        })
        val union = paths union delta
        val distinct = union.distinct
        distinct
      })

      reslt
    })

    act shouldBe alphaEqTo(exp)
  })

  "while loop #1" in withDefaultFlinkEnv(implicit flink => {
    val act = testPipeline(reify {
      val N = this.N
      val edges = this.edges
      val paths = FlinkDataSet(edges)

      val i$1 = 0
      @whileLoop def while$1(i: Int, paths: DataBag[Edge[Int]]): DataBag[Edge[Int]] = {
        val hasNext = i < N

        @loopBody def body$1(): DataBag[Edge[Int]] = {
          val i$2 = i + 1
          val delta = comprehension[Edge[Int], DataBag]({
            val e1 = generator[Edge[Int], DataBag]({
              paths
            })
            val e2 = generator[Edge[Int], DataBag]({
              paths
            })
            guard({
              val dst = e1.dst
              val src = e2.src
              val eqs = dst == src
              eqs
            })
            head[Edge[Int]]({
              val src = e1.src
              val dst = e2.dst
              val edg = Edge(src, dst)
              edg
            })
          })
          val union = paths union delta
          val distinct = union.distinct
          while$1(i$2, distinct)
        }

        @suffix def suffix$1(): DataBag[Edge[Int]] = {
          paths
        }

        if (hasNext) body$1()
        else suffix$1()
      }

      while$1(i$1, paths)
    })

    val exp = dscfPipeline(reify {
      val N = this.N
      val edges = this.edges
      val paths = FlinkDataSet(edges)

      val reslt = FlinkNtv.iterate(paths)(N, paths => {
        val delta = comprehension[Edge[Int], DataBag]({
          val e1 = generator[Edge[Int], DataBag]({
            paths
          })
          val e2 = generator[Edge[Int], DataBag]({
            paths
          })
          guard({
            val dst = e1.dst
            val src = e2.src
            val eqs = dst == src
            eqs
          })
          head[Edge[Int]]({
            val src = e1.src
            val dst = e2.dst
            val edg = Edge(src, dst)
            edg
          })
        })
        val union = paths union delta
        val distinct = union.distinct
        distinct
      })

      reslt
    })

    act shouldBe alphaEqTo(exp)
  })

  "while loop #2" in withDefaultFlinkEnv(implicit flink => {
    val act = testPipeline(reify {
      val N = this.N
      val edges = this.edges
      val paths = FlinkDataSet(edges)

      val i$1 = 0
      @whileLoop def while$1(i: Int, paths: DataBag[Edge[Int]]): DataBag[Edge[Int]] = {
        val hasNext = i < N

        @loopBody def body$1(): DataBag[Edge[Int]] = {
          val i$2 = i + 1
          val delta = comprehension[Edge[Int], DataBag]({
            val e1 = generator[Edge[Int], DataBag]({
              paths
            })
            val e2 = generator[Edge[Int], DataBag]({
              paths
            })
            guard({
              val dst = e1.dst
              val src = e2.src
              val eqs = dst == src
              eqs
            })
            head[Edge[Int]]({
              val src = e1.src
              val dst = e2.dst
              val edg = Edge(src, dst)
              edg
            })
          })
          val grpKey = (e: Edge[Int]) => {
            val src = e.src
            src
          }
          val groups = delta.groupBy(grpKey)
          val result = comprehension[Edge[Int], DataBag]({
            val group = generator[Group[Int, DataBag[Edge[Int]]], DataBag]({
              groups
            })
            head[Edge[Int]]({
              val src = group.key
              val xs1 = group.values
              val rs1 = xs1.size
              val dst = rs1.toInt
              val edg = Edge(src, dst)
              edg
            })
          })
          while$1(i$2, result)
        }

        @suffix def suffix$1(): DataBag[Edge[Int]] = {
          paths
        }

        if (hasNext) body$1()
        else suffix$1()
      }

      while$1(i$1, paths)
    })

    val exp = dscfPipeline(reify {
      val N = this.N
      val edges = this.edges
      val paths = FlinkDataSet(edges)

      val reslt = FlinkNtv.iterate(paths)(N, paths => {
        val delta = comprehension[Edge[Int], DataBag]({
          val e1 = generator[Edge[Int], DataBag]({
            paths
          })
          val e2 = generator[Edge[Int], DataBag]({
            paths
          })
          guard({
            val dst = e1.dst
            val src = e2.src
            val eqs = dst == src
            eqs
          })
          head[Edge[Int]]({
            val src = e1.src
            val dst = e2.dst
            val edg = Edge(src, dst)
            edg
          })
        })
        val grpKey = (e: Edge[Int]) => {
          val src = e.src
          src
        }
        val groups = delta.groupBy(grpKey)
        val result = comprehension[Edge[Int], DataBag]({
          val group = generator[Group[Int, DataBag[Edge[Int]]], DataBag]({
            groups
          })
          head[Edge[Int]]({
            val src = group.key
            val xs1 = group.values
            val rs1 = xs1.size
            val dst = rs1.toInt
            val edg = Edge(src, dst)
            edg
          })
        })
        result
      })

      reslt
    })

    act shouldBe alphaEqTo(exp)
  })
}
