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

import api.spark.SparkExp
import compiler.BaseCompilerSpec
import compiler.RuntimeCompiler
import compiler.SparkCompiler
import test.schema.Graphs._

import java.util.UUID

class SparkSpecializeLambdaSpec extends BaseCompilerSpec with SparkAware {

  override val compiler = new RuntimeCompiler with SparkCompiler

  import compiler._
  import u.reify

  lazy val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(
      Core.anf,
      collectFirstLambda,
      SparkSpecializeOps.specializeLambda.timed
    ).compose(_.tree)

  lazy val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.anf, collectFirstLambda).compose(_.tree)

  lazy val collectFirstLambda = TreeTransform("SparkSpecializeLambdaSpec.collectFirstLambda", tree =>
    (tree collect {
      case t: u.Function => t
    }).head)

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

  "selection #1" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      (y: (Int, Double, String)) => {
        val a = y._2
        val b = y._3
        val c = a > 1
        val d = b startsWith "a"
        val e = c && d
        e
      }
    })

    val exp = anfPipeline(reify {
      (y: SparkExp.Root) => {
        val a = SparkExp.proj(y, "_2")
        val b = SparkExp.proj(y, "_3")
        val c = SparkExp.gt(a, 1)
        val d = SparkExp.startsWith(b, "a")
        val e = SparkExp.and(c, d)
        e
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "selection #2" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      (y: (Float, Double, String)) => {
        val a = y._1
        val b = y._2
        val c = a - b
        val d = c > 1.0
        val e = -1.0 >= c
        val f = d || e
        val g = !f
        g
      }
    })

    val exp = anfPipeline(reify {
      (y: SparkExp.Root) => {
        val a = SparkExp.proj(y, "_1")
        val b = SparkExp.proj(y, "_2")
        val c = SparkExp.minus(a, b)
        val d = SparkExp.gt(c, 1.0)
        val e = SparkExp.geq(-1.0, c)
        val f = SparkExp.or(d, e)
        val g = SparkExp.not(f)
        g
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "projection #1" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      (x: ((Int, Double), String)) => {
        val a = x._1
        val b = a._2
        b
      }
    })

    val exp = anfPipeline(reify {
      (x: SparkExp.Root) => {
        val a = SparkExp.proj(x, "_1")
        val b = SparkExp.proj(a, "_2")
        b
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "projection #2" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      (y: (Int, Double, String)) => {
        val a = y._2
        val b = y._3
        val r = (a, b)
        r
      }
    })

    val exp = anfPipeline(reify {
      (y: SparkExp.Root) => {
        val a = SparkExp.proj(y, "_2")
        val b = SparkExp.proj(y, "_3")
        val r = SparkExp.struct("_1", "_2")(a, b)
        r
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "projection #3" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      (y: (String, Int)) => {
        val a = y._2
        val r = Edge(a, a)
        r
      }
    })

    val exp = anfPipeline(reify {
      (y: SparkExp.Root) => {
        val a = SparkExp.proj(y, "_2")
        val r = SparkExp.struct("src", "dst")(a, a)
        r
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "projection #4" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      (y: (Int, Int, String)) => {
        val a = y._2
        val b = y._3
        val c = LabelledEdge(a, b, "->")
        val d = LabelledEdge(b, a, "<-")
        val r = (c, d)
        r
      }
    })

    val exp = anfPipeline(reify {
      (y: SparkExp.Root) => {
        val a = SparkExp.proj(y, "_2")
        val b = SparkExp.proj(y, "_3")
        val c = SparkExp.struct("src", "dst", "label")(a, b, "->")
        val d = SparkExp.struct("src", "dst", "label")(b, a, "<-")
        val r = SparkExp.struct("_1", "_2")(c, d)
        r
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "projection #5" in withDefaultSparkSession(implicit spark => {
    val act = testPipeline(reify {
      (y: Long) => {
        y
      }
    })

    val exp = anfPipeline(reify {
      (y: SparkExp.Root) => {
        y
      }
    })

    act shouldBe alphaEqTo(exp)
  })
}
