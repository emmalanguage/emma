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
package compiler.spark

import api.SparkDataset
import api.spark.SparkExp
import api.spark.SparkNtv
import api.spark.SparkOps
import compiler.BaseCompilerSpec
import compiler.RuntimeCompiler
import compiler.SparkCompiler
import test.schema.Graphs._

import java.util.UUID

class SparkSpecializeOpsSpec extends BaseCompilerSpec with SparkAware {

  override val compiler = new RuntimeCompiler with SparkCompiler

  import compiler._
  import u.reify

  lazy val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.dscf,
      tree => time(SparkSpecializeSupport.specializeOps(tree), "specializeOps")
    ).compose(_.tree)

  lazy val dscfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.dscf).compose(_.tree)

  lazy val collectFirstLambda: u.Tree => u.Tree = tree => (tree collect {
    case t: u.Function => t
  }).head

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
      def $run(..$prs)(implicit spark: ${SparkAPI.SparkSession}) = $tree
    }
    """
  }

  type Seq1 = (String, Int)
  type Seq2 = (Int, Double, String)
  type Seq3 = Edge[Int]

  val seq1 = for (c <- '0' to 'z') yield c.toLower.toString -> c.toInt
  val seq2 = for (c <- '0' to 'z') yield (c.toInt, Math.PI, c.toString)
  val seq3 = for (c <- 'a' to 'y') yield Edge(c.toInt, c.toInt + 1)

  "select" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      val x1 = this.seq2
      val x2 = SparkDataset(x1)
      val f1 = (y: Seq2) => {
        val y1 = y._2
        val y2 = y._3
        val y3 = y1 > 1
        val y4 = y2 startsWith "a"
        val y5 = y3 && y4
        y5
      }
      val x4 = x2.withFilter(f1)
      x4
    })

    val exp = dscfPipeline(reify {
      val x1 = this.seq2
      val x2 = SparkDataset(x1)
      val p1 = (y: String) => {
        val y1 = SparkExp.rootProj(y, "_2")
        val y2 = SparkExp.rootProj(y, "_3")
        val y3 = SparkExp.gt(y1, 1)
        val y4 = SparkExp.startsWith(y2, "a")
        val y5 = SparkExp.and(y3, y4)
        val y6 = SparkExp.rootStruct("_1")(y5)
        y6
      }
      val x4 = SparkNtv.select(p1)(x2)
      x4
    })

    act shouldBe alphaEqTo(exp)
  })

  "project" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      val x1 = this.seq1
      val x2 = SparkDataset(x1)
      val f1 = (y: Seq1) => {
        val y1 = y._1
        y1
      }
      val x4 = x2.map(f1)
      x4
    })

    val exp = dscfPipeline(reify {
      val x1 = this.seq1
      val x2 = SparkDataset(x1)
      val f1 = (y: String) => {
        val y1 = SparkExp.rootProj(y, "_1")
        val y2 = SparkExp.rootStruct("_1")(y1)
        y2
      }
      val x4 = SparkNtv.project[Seq1, String](f1)(x2)
      x4
    })

    act shouldBe alphaEqTo(exp)
  })

  "join" - {
    "#1" in withDefaultSparkSession(implicit spark => {
      val act = testPipeline(reify {
        val s1 = this.seq1
        val s2 = this.seq2
        val x1 = SparkDataset(s1)
        val x2 = SparkDataset(s2)
        val f1 = (u: Seq1) => {
          val y1 = u._2
          y1
        }
        val f2 = (v: Seq2) => {
          val z1 = v._1
          z1
        }
        val x3 = SparkOps.equiJoin(f1, f2)(x1, x2)
        x3
      })

      val exp = dscfPipeline(reify {
        val s1 = this.seq1
        val s2 = this.seq2
        val x1 = SparkDataset(s1)
        val x2 = SparkDataset(s2)
        val f1 = (u: String) => {
          val y1 = SparkExp.rootProj(u, "_2")
          val y2 = SparkExp.rootStruct("_1")(y1)
          y2
        }
        val f2 = (v: String) => {
          val z1 = SparkExp.rootProj(v, "_1")
          val z2 = SparkExp.rootStruct("_1")(z1)
          z2
        }
        val x3 = SparkNtv.equiJoin[Seq1, Seq2, Int](f1, f2)(x1, x2)
        x3
      })

      act shouldBe alphaEqTo(exp)
    })

    "#2" in withDefaultSparkSession(implicit spark => {
      val act = testPipeline(reify {
        val s1 = this.seq3
        val x1 = SparkDataset(s1)
        val f1 = (u: Seq3) => {
          val y1 = u.dst
          y1
        }
        val f2 = (v: Seq3) => {
          val z1 = v.src
          z1
        }
        val f3 = (w: (Seq3, Seq3)) => {
          val t1 = w._1
          val t2 = w._2
          val t3 = t1.src
          val t4 = t2.dst
          val t5 = Edge(t3, t4)
          t5
        }
        val x3 = SparkOps.equiJoin(f1, f2)(x1, x1)
        val x4 = x3.map(f3)
        x4
      })

      val exp = dscfPipeline(reify {
        val s1 = this.seq3
        val x1 = SparkDataset(s1)
        val f1 = (u: String) => {
          val y1 = SparkExp.rootProj(u, "dst")
          val r1 = SparkExp.rootStruct("_1")(y1)
          r1
        }
        val f2 = (v: String) => {
          val z1 = SparkExp.rootProj(v, "src")
          val r2 = SparkExp.rootStruct("_1")(z1)
          r2
        }
        val f3 = (w: String) => {
          val t1 = SparkExp.rootProj(w, "_1")
          val t2 = SparkExp.rootProj(w, "_2")
          val t3 = SparkExp.nestProj(t1, "src")
          val t4 = SparkExp.nestProj(t2, "dst")
          val t5 = SparkExp.rootStruct("src", "dst")(t3, t4)
          t5
        }
        val x3 = SparkNtv.equiJoin[Seq3, Seq3, Int](f1, f2)(x1, x1)
        val x4 = SparkNtv.project[(Seq3, Seq3), Seq3](f3)(x3)
        x4
      })

      act shouldBe alphaEqTo(exp)
    })
  }
}
