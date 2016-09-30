package org.emmalanguage
package compiler.lang.core

import api.DataBag
import compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for order disambiguation. */
@RunWith(classOf[JUnitRunner])
class OrderSpec extends BaseCompilerSpec {

  import compiler._

  val liftPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf
    ).compose(_.tree)

  val dis: u.Tree => u.Tree = tree => Backend.order(tree)._1

  val disamb: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      tree => time(dis(tree), "disambiguate")
    ).compose(_.tree)

  "order disambiguation" - {

    "only driver" in {

      val inp = u.reify {
        val f = (x: Int) => x + 1
        f(41)
      }

      val exp = inp

      disamb(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "ambiguous" in {

      val inp = u.reify {
        val f = (x: Int) => x + 1
        val x = f(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      val exp = u.reify {
        val f = (x: Int) => x + 1
        val f$high = (x: Int) => x + 1
        val x = f(6)
        val b = DataBag(Seq(1))
        b.map(f$high)
      }

      disamb(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "chain of lambda refs" in {

      val inp = u.reify {
        val g = (x: Int) => x + 2
        val f = (x: Int) => x + g(4)
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      val exp = u.reify {
        val g = (x: Int) => x + 2
        val g$high = (x: Int) => x + 2
        val f = (x: Int) => x + g$high(4)
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      disamb(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "long chain of lambda refs" in {

      val inp = u.reify {
        val h = (x: Int) => x + 3
        val g = (x: Int) => h(x)
        val f = (x: Int) => x + g(4)
        val y = h(5)
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      val exp = u.reify {
        val h = (x: Int) => x + 3
        val h$high = (x: Int) => x + 3
        val g = (x: Int) => h(x)
        val g$high = (x: Int) => h$high(x)
        val f = (x: Int) => x + g$high(4)
        val y = h(5)
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      disamb(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "higher-order function executed in driver" in {

      val inp = u.reify {
        val h = (l: Int => Int) => {
          val ir = (x: Int) => l(x)
          ir
        }
        val g = (x: Int) => x + 2
        val f = h(g)
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      val exp = u.reify {
        val h = (l: Int => Int) => {
          val ir = (x: Int) => l(x)
          val ir$high = (x: Int) => l(x)
          ir
        }
        val h$high = (l: Int => Int) => {
          val ir = (x: Int) => l(x)
          ir
        }
        val g = (x: Int) => x + 2
        val g$high = (x: Int) => x + 2
        val f = h$high(g$high) // Note that this is a false positive for h: ideally this should be h(g$high)
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      disamb(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "higher-order function called from high" in {

      val inp = u.reify {
        val h = (l: Int => Int) => {
          val ir = (x: Int) => l(x)
          ir
        }
        val m = (x: Int) => x + 3
        val g = (x: Int) => h(m)(x)
        val f = h(g)
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      val exp = u.reify {
        val h = (l: Int => Int) => {
          val ir = (x: Int) => l(x)
          val ir$high = (x: Int) => l(x)
          ir
        }
        val h$high = (l: Int => Int) => {
          val ir = (x: Int) => l(x)
          ir
        }
        val m = (x: Int) => x + 3
        val m$high = (x: Int) => x + 3
        val g = (x: Int) => {
          val ir = h(m)
          val ir$high = h$high(m$high)
          ir(x)
        }
        val g$high = (x: Int) => h$high(m$high)(x)
        val f = h$high(g$high) // false positive, see above
        val x = g(6)
        val b = DataBag(Seq(1))
        b.map(f)
      }

      disamb(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

  }
}
