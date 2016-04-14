package eu.stratosphere.emma.compiler.ir.lnf

import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `LNF.dce` transformation. */
@RunWith(classOf[JUnitRunner])
class DCESpec extends BaseCompilerSpec with TreeEquality {

  import compiler.universe._
  import eu.stratosphere.emma.testschema.Marketing._

  def typeCheckAndDCE[T](expr: Expr[T]): Tree = {
    val pipeline = {
      compiler.typeCheck(_: Tree)
    } andThen {
      compiler.LNF.destructPatternMatches
    } andThen {
      compiler.LNF.resolveNameClashes
    } andThen {
      compiler.LNF.anf
    } andThen {
      compiler.LNF.dce
    }

    pipeline(expr.tree)
  }

  def typeCheck[T](expr: Expr[T]): Tree = {
    compiler.typeCheck(expr.tree)
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

      act shouldEqual exp
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

      act shouldEqual exp
    }
  }
}


