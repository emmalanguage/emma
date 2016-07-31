package eu.stratosphere.emma
package compiler.lang.core

import compiler.BaseCompilerSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `LNF.dce` transformation. */
@RunWith(classOf[JUnitRunner])
class DCESpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  def typeCheckAndDCE[T]: Expr[T] => Tree = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_)
  } andThen {
    Core.destructPatternMatches
  } andThen {
    Core.resolveNameClashes
  } andThen {
    Core.anf
  } andThen {
    time(Core.dce(_), "dce")
  } andThen {
    Owner.at(Owner.enclosing)
  }

  "eliminate unused valdefs" - {

    "directly" in {
      val act = typeCheckAndDCE(reify {
        val x = 15
        15 * t._1
      })

      val exp = typeCheck(reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "transitively" in {
      val act = typeCheckAndDCE(reify {
        val x = 15
        val y = 2 * x
        15 * t._1
      })

      val exp = typeCheck(reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }
}


