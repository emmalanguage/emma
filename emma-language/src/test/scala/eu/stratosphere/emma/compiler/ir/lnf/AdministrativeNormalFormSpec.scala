package eu.stratosphere.emma
package compiler.ir.lnf

import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class AdministrativeNormalFormSpec extends BaseCompilerSpec {

  import scala.reflect.runtime.universe._

  def typeCheckAndNormalize[T](expr: Expr[T]): Tree = {
    compiler.LNF.anf(compiler.typeCheck(expr.tree))
  }

  def typeCheck[T](expr: Expr[T]): Tree = {
    compiler.typeCheck(expr.tree)
  }

  // common value definitions used below
  val x = 42
  val y = "The answer to life, the universe and everything"
  val t = (x, y)

  "field selections" - {

    "as argument" in {
      val t1 = typeCheckAndNormalize(reify {
        15 * t._1
      })

      val t2 = typeCheck(reify {
        val x$01 = t._1
        15 * x$01
      })

      compiler.LNF.eq(t1, t2) shouldBe true
    }

    "as selection" in {
      val t1 = typeCheckAndNormalize(reify {
        t._1 * 15
      })

      val t2 = typeCheck(reify {
        val x$01 = t._1
        x$01 * 15
      })

      compiler.LNF.eq(t1, t2) shouldBe true
    }

    "package selections" in {
      val t1 = typeCheckAndNormalize(reify {
        val bag = eu.stratosphere.emma.api.DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.fetch())
      })

      val t2 = typeCheck(reify {
        val x$01 = Seq(1, 2, 3)
        val bag = eu.stratosphere.emma.api.DataBag(x$01)
        scala.Predef.println(bag.fetch())
      })

      compiler.LNF.eq(t1, t2) shouldBe true
    }
  }

  "complex arguments" - {
    "lhs" in {
      val t1 = typeCheckAndNormalize(reify {
        y.substring(y.indexOf('l') + 1)
      })

      val t2 = typeCheckAndNormalize(reify {
        val x$01 = y.indexOf('l')
        val x$02 = x$01 + 1
        y.substring(x$02)
      })

      compiler.LNF.eq(t1, t2) shouldBe true
    }
  }
}
