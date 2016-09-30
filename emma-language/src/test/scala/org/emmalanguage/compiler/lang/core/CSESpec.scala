package org.emmalanguage
package compiler.lang.core

import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import eu.stratosphere.emma.api.DataBag
import testschema.Literature._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `LNF.cse` transformation. */
@RunWith(classOf[JUnitRunner])
class CSESpec extends BaseCompilerSpec {

  import compiler._

  val csePipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      tree => time(Core.cse(tree), "cse")
    ).compose(_.tree)

  val lnfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf
    ).compose(_.tree)

  "field selections" - {

    "as argument" in {
      val act = csePipeline(u.reify {
        15 * t._1
      })

      val exp = idPipeline(u.reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "as selection" in {
      val act = csePipeline(u.reify {
        t._1 * 15
      })

      val exp = idPipeline(u.reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = x$2 * 15
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "package selections" in {
      val act = csePipeline(u.reify {
        val bag = eu.stratosphere.emma.api.DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.fetch())
      })

      val exp = idPipeline(u.reify {
        val x$1 = Seq(1, 2, 3)
        val bag = eu.stratosphere.emma.api.DataBag(x$1)
        val x$2 = bag.fetch()
        val x$3 = scala.Predef.println(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "complex arguments" - {
    "lhs" in {
      val act = csePipeline(u.reify {
        y.substring(y.indexOf('l') + 1)
      })

      val exp = csePipeline(u.reify {
        val y$1 = y
        val x$1 = y$1.indexOf('l')
        val x$2 = x$1 + 1
        val x$3 = y$1.substring(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "nested blocks" in {
    val act = csePipeline(u.reify {
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

    val exp = idPipeline(u.reify {
      val y$1 = y
      val b$1 = y$1.indexOf('a')
      val a = b$1 + 15
      val r = a + a
      r
    })

    act shouldBe alphaEqTo(exp)
  }

  "copy propagation" in {
    val act = csePipeline(u.reify {
      val a = 42
      val b = a
      val c = b
      c
    })

    val exp = u.Block(Nil, u.reify(42).tree)
    act shouldBe alphaEqTo(exp)
  }

  "lambdas" in {
    val act = csePipeline(u.reify {
      val f = (i: Int) => i + 1
      val g = (i: Int) => i + 1
      val xs = Seq(1, 2, 3)
      xs.map(f).map(g)
    })

    val exp = idPipeline(u.reify {
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
    val act = csePipeline(u.reify {
      val xs = comprehension[(Character, Character), DataBag] {
        val x = generator[Character, DataBag](DataBag(hhCrts))
        val y = generator[Character, DataBag](DataBag(hhCrts))
        head (x, y)
      }
      val sze = xs.size
      val cnt = xs.count{ case (x, y) => x.name == y.name }
      sze / cnt.toDouble
    })

    val exp = lnfPipeline(u.reify {
      val xs = comprehension[(Character, Character), DataBag] {
        val x = generator[Character, DataBag](DataBag(hhCrts))
        val y = generator[Character, DataBag](DataBag(hhCrts))
        head (x, y)
      }
      
      val fn$1 = (x$2: (Character, Character)) => {
        1L
      }

      // used twice: once in the `sze`, and once in the `cde` folds
      val fn$2 = (x$3: Long, x$1: Long) => {
        val `+$8` = x$3 + x$1
        `+$8`
      }

      val sze = xs.fold(0L)(fn$1, fn$2)
      
      val fn$3 = (x$5: (Character, Character)) => {
        val fn$4 = (x0$1: (Character, Character)) => {
          val x = x0$1._1
          val y = x0$1._2
          val name$1 = x.name
          val name$2 = y.name
          val `==$1` = name$1 == name$2
          `==$1`
        }
        (fn$4(x$5) compare false).toLong
      }

      val cde = xs.fold(0L)(fn$3, fn$2)

      sze / cde.toDouble
    })

    act shouldBe alphaEqTo(exp)
  }
}
