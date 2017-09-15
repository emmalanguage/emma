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
package compiler.backend

import api._
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import compiler.ir.DSCFAnnotations._

import collection.breakOut

/** A spec for binding context approximation. */
class ContextBCtxSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  //----------------------------------------------------------------------------
  // Test Helper Methods
  //----------------------------------------------------------------------------

  val liftPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true, withPost = false)(Core.lift).compose(_.tree)

  val bCtxtMap: u.Tree => Map[u.TermSymbol, BCtx] = tree =>
    time(Context.bCtxMap(ControlFlow.cfg(tree), false), "bCtxtMap")

  val symbols: u.Tree => Map[String, u.TermSymbol] = tree =>
    ControlFlow.cfg(tree).data.nodes.map(s => s.name.toString -> s)(breakOut)

  //----------------------------------------------------------------------------
  // Test Data
  //----------------------------------------------------------------------------

  // Seahorse Valley
  val (cRe, cIm) = (-0.75, 0.1)
  val N = 1000
  val f = 0.01
  val ps = DataBag(-N to N)
  val zoom1 = (x: Int) => DataBag(-x to x).map(_ / f)
  val zoom2 = (x: Int, f: Double) => DataBag(-x to x).map(_ / f)

  "only driver" in {
    val snippet = liftPipeline(reify {
      val f1 = (p1: Int) => {
        val x1 = p1 + 1
        x1
      }
      val x2 = f1(41)
      val x3 = (x2, f1)
      x3
    })

    val C = bCtxtMap(snippet)
    val S = symbols(snippet)

    for (symNme <- Seq("f1", "p1", "x1", "x2", "x3"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Driver)
  }

  "engine lambda" in {
    val snippet = liftPipeline(reify {
      val ps = this.ps
      val f1 = (p1: Int) => {
        val x2 = p1 % 4
        x2
      }
      val x1 = ps.groupBy(f1)
      x1
    })

    val C = bCtxtMap(snippet)
    val S = symbols(snippet)

    for (symNme <- Seq("ps", "f1", "x1"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Driver)

    for (symNme <- Seq("p1", "x2"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Engine)
  }

  "ambiguous lambda" in {
    val snippet = liftPipeline(reify {
      val ps = this.ps
      val f1 = (p1: Int) => {
        val x5 = p1 % 4
        x5
      }
      val x1 = f1(5)
      val x2 = ps.groupBy(f1)
      val x3 = (f1, x1, x2)
      x3
    })

    val C = bCtxtMap(snippet)
    val S = symbols(snippet)

    for (symNme <- Seq("ps", "f1", "x1", "x2", "x3"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Driver)

    for (symNme <- Seq("p1", "x5"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Ambiguous)
  }

  "chain of lambdas (only driver)" in {
    val snippet = liftPipeline(reify {
      val f1 = (p1: Int) => {
        val x3 = p1 + 2
        x3
      }
      val f2 = (p2: Int) => {
        val x4 = f1(4)
        val x5 = p2 + x4
        x5
      }
      val x1 = f2(6)
      val x2 = (f1, f2, x1)
      x2
    })

    val C = bCtxtMap(snippet)
    val S = symbols(snippet)

    for (symNme <- Seq("f1", "f2", "p1", "p2", "x1", "x2", "x3", "x4", "x5"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Driver)
  }

  "chain of lambdas (ambiguous)" in {
    val snippet = liftPipeline(reify {
      val ps = this.ps
      val f1 = (p1: Int) => {
        val x4 = p1 + 2
        x4
      }
      val f2 = (p2: Int) => {
        val x5 = f1(4)
        val x6 = p2 + x5
        x6
      }
      val x1 = f2(6)
      val x2 = ps.groupBy(f1)
      val x3 = (f1, f2, x1, x2)
      x3
    })

    val C = bCtxtMap(snippet)
    val S = symbols(snippet)

    for (symNme <- Seq("ps", "f1", "f2", "p2", "x1", "x2", "x3", "x5", "x6"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Driver)

    for (symNme <- Seq("p1", "x4"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Ambiguous)
  }

  "driver with control flow" in {
    val snippet = liftPipeline(reify {
      val cRe = this.cRe
      val cIm = this.cIm

      @whileLoop def while$r1(i: Int, zIm: Double, zRe: Double): (Double, Double) = {
        val cond = i < 100

        @loopBody def body$r1(): (Double, Double) = {
          val rSq = zRe * zRe
          val iSq = zIm * zIm
          val sRe = rSq - iSq
          val zRI = zRe * zIm
          val sIm = 2 * zRI
          val zRe$r2 = sRe + cRe
          val zIm$r2 = sIm + cIm
          val i$r2 = i + 1
          while$r1(i$r2, zIm$r2, zRe$r2)
        }

        @suffix def suffix$r1(): (Double, Double) = {
          val res = (zRe, zIm)
          res
        }

        if (cond) body$r1() else suffix$r1()
      }

      while$r1(0, 0.0, 0.0)
    })

    val C = bCtxtMap(snippet)
    val S = symbols(snippet)

    for {
      symNme <- Seq(
        "cRe", "cIm",
        "i", "zIm", "zRe",
        "cond",
        "rSq", "iSq", "sRe", "zRI", "zRe$r2", "zIm$r2", "i$r2",
        "res"
      )
    } withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Driver)
  }

  "simple driver with a comprehension" in {
    val snippet = liftPipeline(reify {
      val ps = this.ps
      val ys = comprehension[Double, DataBag] {
        val x = generator[Int, DataBag] {
          ps
        }
        val y = generator[Int, DataBag] {
          ps
        }
        val u = generator[Double, DataBag] {
          val z1 = zoom1(x)
          z1
        }
        val v = generator[Double, DataBag] {
          val z2 = zoom2(y, u)
          z2
        }
        head[Double] {
          val w = u * v
          w
        }
      }

      ys
    })

    val C = bCtxtMap(snippet)
    val S = symbols(snippet)

    for (symNme <- Seq("ps", "ys"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Driver)
    for (symNme <- Seq("x", "y", "u", "z1", "z2", "w"))
      withClue(s"bCtxMap($symNme):")(C(S(symNme)) shouldBe BCtx.Engine)
  }
}
