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

import api.DataBag
import api.SparkDataset
import api.backend.LocalOps
import api.spark._
import compiler.BaseCompilerSpec
import compiler.RuntimeCompiler
import compiler.SparkCompiler
import test.schema.Literature._
import test.schema.Graphs._

import java.util.UUID

class SparkSpecializeOpsSpec extends BaseCompilerSpec with SparkAware {

  override val compiler = new RuntimeCompiler with SparkCompiler

  import compiler._
  import u.reify

  lazy val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(
      Core.dscf,
      SparkSpecializeOps.specializeOps.timed
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
  type Seq4 = (DataBag[Seq1], DataBag[Seq2])

  val seq1 = for (c <- '0' to 'z') yield c.toLower.toString -> c.toInt
  val seq2 = for (c <- '0' to 'z') yield (c.toInt, Math.PI, c.toString)
  val seq3 = for (c <- 'a' to 'y') yield Edge(c.toInt, c.toInt + 1)
  val seq4 = Seq((DataBag(seq1), DataBag(seq2)))

  val proj = (x: Seq3) => x.dst

  "check for supported types" in {
    SparkSpecializeOps.supported(api.Type[Book]) shouldBe true
    SparkSpecializeOps.supported(api.Type[Character]) shouldBe true
    SparkSpecializeOps.supported(api.Type[(Book, Option[Character])]) shouldBe true
    SparkSpecializeOps.supported(api.Type[LabelledEdge[Book, Array[Character]]]) shouldBe true
    SparkSpecializeOps.supported(api.Type[LabelledEdge[String, java.lang.Integer]]) shouldBe true
    SparkSpecializeOps.supported(api.Type[LabelledEdge[java.lang.String, Int]]) shouldBe true
    SparkSpecializeOps.supported(api.Type[Seq1]) shouldBe true
    SparkSpecializeOps.supported(api.Type[Seq2]) shouldBe true
    SparkSpecializeOps.supported(api.Type[Seq3]) shouldBe true
    SparkSpecializeOps.supported(api.Type[(Seq2, Seq[Seq2])]) shouldBe true

    SparkSpecializeOps.supported(api.Type[LabelledEdge[java.lang.Boolean, Char]]) shouldBe false
    SparkSpecializeOps.supported(api.Type[(Range, Seq[Seq1])]) shouldBe false

    assert(encoderForType[Book].isInstanceOf[AnyRef])
    assert(encoderForType[Character].isInstanceOf[AnyRef])
    assert(encoderForType[(Book, Option[Character])].isInstanceOf[AnyRef])
    assert(encoderForType[LabelledEdge[Book, Array[Character]]].isInstanceOf[AnyRef])
    assert(encoderForType[LabelledEdge[String, java.lang.Integer]].isInstanceOf[AnyRef])
    assert(encoderForType[LabelledEdge[java.lang.String, Int]].isInstanceOf[AnyRef])
    assert(encoderForType[Seq1].isInstanceOf[AnyRef])
    assert(encoderForType[Seq2].isInstanceOf[AnyRef])
    assert(encoderForType[Seq3].isInstanceOf[AnyRef])
    assert(encoderForType[((Int, Double, String), Seq[Edge[Int]])].isInstanceOf[AnyRef])

    assertThrows[UnsupportedOperationException] {
      encoderForType[LabelledEdge[java.lang.Boolean, Char]].isInstanceOf[AnyRef]
    }
    assertThrows[Throwable] {
      encoderForType[(Range, Seq[(String, Int)])].isInstanceOf[AnyRef]
    }
  }

  "specializeOps" - {
    "should specialize `withFilter` calls (1)" in withDefaultSparkSession(implicit spark => {
      val act = testPipeline(reify {
        val x1 = this.seq2
        val x2 = SparkDataset(x1)
        val f1 = (y: Seq2) => {
          val y1 = y._2
          val y2 = y._3
          val y3 = y1 > 1
          val y4 = y2 startsWith "a"
          val y5 = y2 contains "b"
          val y6 = y3 && y4
          val y7 = y5 && y6
          y7
        }
        val x4 = x2.withFilter(f1)
        x4
      })

      val exp = dscfPipeline(reify {
        val x1 = this.seq2
        val x2 = SparkDataset(x1)
        val p1 = (y: SparkExp.Root) => {
          val y1 = SparkExp.proj(y, "_2")
          val y2 = SparkExp.proj(y, "_3")
          val y3 = SparkExp.gt(y1, 1)
          val y4 = SparkExp.startsWith(y2, "a")
          val y5 = SparkExp.contains(y2, "b")
          val y6 = SparkExp.and(y3, y4)
          val y7 = SparkExp.and(y5, y6)
          y7
        }
        val x4 = SparkNtv.select(p1)(x2)
        x4
      })

      act shouldBe alphaEqTo(exp)
    })

    "should specialize `withFilter` calls (2)" in withDefaultSparkSession(implicit spark => {
      val act = testPipeline(reify {
        val x1 = this.seq2
        val x2 = SparkDataset(x1)
        val f1 = (y: Seq2) => {
          val y1 = y._2
          val y2 = y._3
          val y3 = y1 <= 1
          val y4 = y2 != null
          val y5 = y2 == null
          val y6 = y3 && y4
          val y7 = y5 && y6
          y7
        }
        val x4 = x2.withFilter(f1)
        x4
      })

      val exp = dscfPipeline(reify {
        val x1 = this.seq2
        val x2 = SparkDataset(x1)
        val p1 = (y: SparkExp.Root) => {
          val y1 = SparkExp.proj(y, "_2")
          val y2 = SparkExp.proj(y, "_3")
          val y3 = SparkExp.leq(y1, 1)
          val y4 = SparkExp.isNotNull(y2)
          val y5 = SparkExp.isNull(y2)
          val y6 = SparkExp.and(y3, y4)
          val y7 = SparkExp.and(y5, y6)
          y7
        }
        val x4 = SparkNtv.select(p1)(x2)
        x4
      })

      act shouldBe alphaEqTo(exp)
    })

    "should specialize `map` calls" in withDefaultSparkSession(implicit spark => {
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
        val f1 = (y: SparkExp.Root) => {
          val y1 = SparkExp.proj(y, "_1")
          y1
        }
        val x4 = SparkNtv.project[Seq1, String](f1)(x2)
        x4
      })

      act shouldBe alphaEqTo(exp)
    })

    "should specialize `equiJoin` calls" - {
      "example #1" in withDefaultSparkSession(implicit spark => {
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
          val f1 = (u: SparkExp.Root) => {
            val y1 = SparkExp.proj(u, "_2")
            y1
          }
          val f2 = (v: SparkExp.Root) => {
            val z1 = SparkExp.proj(v, "_1")
            z1
          }
          val x3 = SparkNtv.equiJoin[Seq1, Seq2, Int](f1, f2)(x1, x2)
          x3
        })

        act shouldBe alphaEqTo(exp)
      })

      "example #2" in withDefaultSparkSession(implicit spark => {
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
          val f1 = (u: SparkExp.Root) => {
            val y1 = SparkExp.proj(u, "dst")
            y1
          }
          val f2 = (v: SparkExp.Root) => {
            val z1 = SparkExp.proj(v, "src")
            z1
          }
          val f3 = (w: SparkExp.Root) => {
            val t1 = SparkExp.proj(w, "_1")
            val t2 = SparkExp.proj(w, "_2")
            val t3 = SparkExp.proj(t1, "src")
            val t4 = SparkExp.proj(t2, "dst")
            val t5 = SparkExp.struct("src", "dst")(t3, t4)
            t5
          }
          val x3 = SparkNtv.equiJoin[Seq3, Seq3, Int](f1, f2)(x1, x1)
          val x4 = SparkNtv.project[(Seq3, Seq3), Seq3](f3)(x3)
          x4
        })

        act shouldBe alphaEqTo(exp)
      })
    }

    "should not specialize calls" - {
      "with a non-specializable lambda argument" in withDefaultSparkSession(implicit spark => {
        val act = testPipeline(reify {
          val s1 = this.seq3
          val x1 = SparkDataset(s1)
          val f1 = (u: Seq3) => {
            val y1 = u.dst
            y1
          }
          val f2 = (v: Seq3) => {
            val z1 = proj(v)
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
          val f1 = (u: Seq3) => {
            val y1 = u.dst
            y1
          }
          val f2 = (v: Seq3) => {
            val z1 = proj(v)
            z1
          }
          val f3 = (w: SparkExp.Root) => {
            val t1 = SparkExp.proj(w, "_1")
            val t2 = SparkExp.proj(w, "_2")
            val t3 = SparkExp.proj(t1, "src")
            val t4 = SparkExp.proj(t2, "dst")
            val t5 = SparkExp.struct("src", "dst")(t3, t4)
            t5
          }
          val x3 = SparkOps.equiJoin(f1, f2)(x1, x1)
          val x4 = SparkNtv.project[(Seq3, Seq3), Seq3](f3)(x3)
          x4
        })

        act shouldBe alphaEqTo(exp)
      })

      "in a data-parallel context" in withDefaultSparkSession(implicit spark => {
        val act = testPipeline(reify {
          val s4 = this.seq4
          val x4 = SparkDataset(s4)
          val f1 = (u: Seq1) => {
            val y1 = u._2
            y1
          }
          val f3 = (u: Seq4) => {
            val f2 = (v: Seq2) => {
              val z1 = v._1
              z1
            }
            val x1 = u._1
            val x2 = u._2
            val x3 = LocalOps.equiJoin(f1, f2)(x1, x2)
            x3
          }
          val x5 = x4.map(f3)
          x5
        })

        val exp = dscfPipeline(reify {
          val s4 = this.seq4
          val x4 = SparkDataset(s4)
          val f1 = (u: Seq1) => {
            val y1 = u._2
            y1
          }
          val f3 = (u: Seq4) => {
            val f2 = (v: Seq2) => {
              val z1 = v._1
              z1
            }
            val x1 = u._1
            val x2 = u._2
            val x3 = LocalOps.equiJoin(f1, f2)(x1, x2)
            x3
          }
          val x5 = x4.map(f3)
          x5
        })

        act shouldBe alphaEqTo(exp)
      })
    }
  }
}
