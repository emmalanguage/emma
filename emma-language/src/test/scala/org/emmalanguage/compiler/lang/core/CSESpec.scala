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

import api._
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import test.schema.Literature._

/** A spec for the `LNF.cse` transformation. */
class CSESpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val csePipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      Core.cse.timed
    ).compose(_.tree)

  val lnfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf
    ).compose(_.tree)

  "field selections" - {

    "as argument" in {
      val act = csePipeline(reify {
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

    "as selection" in {
      val act = csePipeline(reify {
        t._1 * 15
      })

      val exp = idPipeline(reify {
        val x$1: this.t.type = t
        val x$2 = x$1._1
        val x$3 = x$2 * 15
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "package selections" in {
      val act = csePipeline(reify {
        val bag = DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.collect())
      })

      val exp = idPipeline(reify {
        val x$1 = Seq(1, 2, 3)
        val bag = DataBag(x$1)
        val x$2 = bag.collect()
        val x$3 = scala.Predef.println(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "complex arguments" - {
    "lhs" in {
      val act = csePipeline(reify {
        y.substring(y.indexOf('l') + 1)
      })

      val exp = csePipeline(reify {
        val y$1: this.y.type = y
        val x$1 = y$1.indexOf('l')
        val x$2 = x$1 + 1
        val x$3 = y$1.substring(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "nested blocks" in {
    val act = csePipeline(reify {
      val z = y
      val a = {
        val b = y.indexOf('a')
        b + 15
      }
      val c = {
        val b = z.indexOf('a')
        b + 15
      }
      a + c
    })

    val exp = idPipeline(reify {
      val y$1 = y
      val b$1 = y$1.indexOf('a')
      val a = b$1 + 15
      val r = a + a
      r
    })

    act shouldBe alphaEqTo(exp)
  }

  "copy propagation" in {
    val act = csePipeline(reify {
      val a = 42
      val b = a
      val c = b
      c
    })

    val exp = u.Block(Nil, reify(42).tree)
    act shouldBe alphaEqTo(exp)
  }

  "lambdas" in {
    val act = csePipeline(reify {
      val f = (i: Int) => i + 1
      val g = (i: Int) => i + 1
      val xs = Seq(1, 2, 3)
      xs.map(f).map(g)
    })

    val exp = idPipeline(reify {
      val f = (i: Int) => {
        val i$1 = i + 1
        i$1
      }
      val xs$1 = Seq(1, 2, 3)
      val cbf = Seq.canBuildFrom[Int]
      val xs$2 = xs$1.map(f)(cbf)
      val xs$3 = xs$2.map(f)(cbf)
      xs$3
    })

    act shouldBe alphaEqTo(exp)
  }

  "for-comprehensions" in {
    val act = csePipeline(reify {
      val xs = comprehension[(Character, Character), DataBag] {
        val x = generator[Character, DataBag](DataBag(hhCrts))
        val y = generator[Character, DataBag](DataBag(hhCrts))
        head (x, y)
      }
      val sze = xs.size
      val cnt = xs.count { case (x1, x2) => x1.name == x2.name }
      sze / cnt.toDouble
    })

    val exp = lnfPipeline(reify {
      val xs = comprehension[(Character, Character), DataBag] {
        val x = generator[Character, DataBag](DataBag(hhCrts))
        val y = generator[Character, DataBag](DataBag(hhCrts))
        head (x, y)
      }

      val sze = xs.size;
      val fn$4 = (x0$1: (Character, Character)) => {
        val x = x0$1._1
        val y = x0$1._2
        val name$1: x.name.type = x.name
        val name$2: y.name.type = y.name
        val `==$1` = name$1 == name$2
        `==$1`
      }
      val cnt = xs.count(fn$4);
      sze / cnt.toDouble
    })

    act shouldBe alphaEqTo(exp)
  }
}
