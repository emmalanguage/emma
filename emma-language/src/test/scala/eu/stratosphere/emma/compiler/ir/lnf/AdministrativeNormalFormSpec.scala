package eu.stratosphere.emma
package compiler.ir.lnf

import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class AdministrativeNormalFormSpec extends BaseCompilerSpec {

  import compiler.universe._

  def typeCheckAndNormalize[T](expr: Expr[T]): Tree = {
    val pipeline = {
      compiler.typeCheck(_: Tree)
    } andThen {
      compiler.LNF.resolveNameClashes
    } andThen {
      compiler.LNF.anf
    }

    pipeline(expr.tree)
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
        val x$1 = AdministrativeNormalFormSpec.this.t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      compiler.LNF.eq(t1, t2) shouldBe true
    }

    "as selection" in {
      val t1 = typeCheckAndNormalize(reify {
        t._1 * 15
      })

      val t2 = typeCheck(reify {
        val x$1 = AdministrativeNormalFormSpec.this.t
        val x$2 = x$1._1
        val x$3 = x$2 * 15
        x$3
      })

      compiler.LNF.eq(t1, t2) shouldBe true
    }

    "package selections" in {
      val t1 = typeCheckAndNormalize(reify {
        val bag = eu.stratosphere.emma.api.DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.fetch())
      })

      val t2 = typeCheck(reify {
        val x$1 = Seq(1, 2, 3)
        val bag = eu.stratosphere.emma.api.DataBag(x$1)
        val x$2 = bag.fetch()
        val x$3 = scala.Predef.println(x$2)
        x$3
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
        val y$1 = AdministrativeNormalFormSpec.this.y
        val y$2 = AdministrativeNormalFormSpec.this.y
        val x$1 = y$2.indexOf('l')
        val x$2 = x$1 + 1
        val x$3 = y$1.substring(x$2)
        x$3
      })

      compiler.LNF.eq(t1, t2) shouldBe true
    }
  }

  "nested blocks" in {
    val t1 = typeCheckAndNormalize(reify {
      val a = {
        val b = y.indexOf('T')
        b + 15
      }
      val c = {
        val b = y.indexOf('a')
        b + 15
      }
    })

    val t2 = typeCheck(reify {
      val y$1 = y
      val b$1 = y$1.indexOf('T')
      val a = b$1 + 15
      val y$2 = y
      val b$2 = y$2.indexOf('a')
      val c = b$2 + 15
    })

    compiler.LNF.eq(t1, t2) shouldBe true
  }
}
