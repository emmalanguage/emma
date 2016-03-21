package eu.stratosphere.emma.compiler.ir.core

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * A spec for alpha-equivalence on trees.
 */
@RunWith(classOf[JUnitRunner])
class EquivalenceSpec extends BaseCompilerSpec {

  import compiler.universe._

  def typeCheck[T](expr: Expr[T]): Tree = {
    compiler.typeCheck(expr.tree)
  }

  // common value definitions used below
  val x = 42
  val y = "The answer to life, the universe and everything"
  val t = (x, y)
  val xs = DataBag(Seq(1, 2, 3))
  val ys = DataBag(Seq(2, 3, 3))

  "simple valdefs and expressions" in {
    val t1 = typeCheck(reify {
      val a$01 = 42 * x
      val a$02 = a$01 * t._1
      15 * a$01 * a$02
    })

    val t2 = typeCheck(reify {
      val b$01 = 42 * x
      val b$02 = b$01 * t._1
      15 * b$01 * b$02
    })

    compiler.Core.eq(t1, t2) shouldBe true
  }

  "conditionals" in {
    val t1 = typeCheck(reify {
      val a$01 = 42 * x
      if (x < 42) x * t._1 else x / a$01
    })

    val t2 = typeCheck(reify {
      val b$01 = 42 * x
      if (x < 42) x * t._1 else x / b$01
    })

    compiler.Core.eq(t1, t2) shouldBe true
  }

  "variable assignment and loops" in {
    val t1 = typeCheck(reify {
      var u = x
      while (u < 20) {
        println(y)
        u = u + 1
      }

      do {
        u = u - 2
      } while (u < 20)
    })

    val t2 = typeCheck(reify {
      var v = x
      while (v < 20) {
        println(y)
        v = v + 1
      }

      do {
        v = v - 2
      } while (v < 20)
    })

    compiler.Core.eq(t1, t2) shouldBe true
  }

  "pattern matching" in {
    val t1 = typeCheck(reify {
      val u = (t, x)
      u match {
        case ((i, j: String), _) => i * 42
      }
    })

    val t2 = typeCheck(reify {
      val v = (t, x)
      v match {
        case ((l, m: String), _) => l * 42
      }
    })

    compiler.Core.eq(t1, t2) shouldBe true
  }
}
