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
package compiler.lang.core

import compiler.BaseCompilerSpec

/** A spec for the `LNF.dce` transformation. */
class DCESpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val dcePipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      DCE.transform.timed
    ).compose(_.tree)

  "eliminate unused valdefs" - {
    "directly" in {
      //noinspection ScalaUnusedSymbol
      val act = dcePipeline(reify {
        val x = 15
        15 * t._1
      })

      val exp = idPipeline(reify {
        val x$1: this.t.type = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "transitively" in {
      //noinspection ScalaUnusedSymbol
      val act = dcePipeline(reify {
        val x = 15
        val y = 2 * x
        15 * t._1
      })

      val exp = idPipeline(reify {
        val x$1: this.t.type = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "don't eliminate" - {
    "unit valdefs" in {
      val act = dcePipeline(reify {
        println("alma")
        val x = 5
        x
      })

      //noinspection ScalaUnusedSymbol
      val exp = idPipeline(reify {
        val res = println("alma")
        val x = 5
        x
      })

      act shouldBe alphaEqTo(exp)
    }

    "implicit valdefs" in {
      val act = dcePipeline(reify {
        implicit val x = 5
        val y = 6
        y
      })

      val exp = idPipeline(reify {
        implicit val x = 5
        val y = 6
        y
      })

      act shouldBe alphaEqTo(exp)
    }
  }
}
