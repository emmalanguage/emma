package eu.stratosphere.emma
package compiler.lang.core

import compiler.BaseCompilerSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `LNF.dce` transformation. */
@RunWith(classOf[JUnitRunner])
class DCESpec extends BaseCompilerSpec {

  import compiler._

  val dcePipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true)(
      ANF.transform,
      tree => time(DCE.transform(tree), "dce")
    ).compose(_.tree)

  "eliminate unused valdefs" - {

    "directly" in {
      val act = dcePipeline(u.reify {
        val x = 15
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

    "transitively" in {
      val act = dcePipeline(u.reify {
        val x = 15
        val y = 2 * x
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
  }
}


