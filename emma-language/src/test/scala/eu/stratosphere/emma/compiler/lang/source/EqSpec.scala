package eu.stratosphere.emma.compiler.lang.source

import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for alpha-equivalence on trees. */
@RunWith(classOf[JUnitRunner])
class EqSpec extends BaseCompilerSpec with TreeEquality {

  import compiler.universe._

  "simple valdefs and expressions" in {
    val lhs = typeCheck(reify {
      val a$01 = 42 * x
      val a$02 = a$01 * t._1
      15 * a$01 * a$02
    })

    val rhs = typeCheck(reify {
      val b$01 = 42 * x
      val b$02 = b$01 * t._1
      15 * b$01 * b$02
    })

    lhs shouldEqual rhs
  }

  "conditionals" in {
    val lhs = typeCheck(reify {
      val a$01 = 42 * x
      if (x < 42) x * t._1 else x / a$01
    })

    val rhs = typeCheck(reify {
      val b$01 = 42 * x
      if (x < 42) x * t._1 else x / b$01
    })

    lhs shouldEqual rhs
  }

  "variable assignment and loops" in {
    val lhs = typeCheck(reify {
      var u = x
      while (u < 20) {
        println(y)
        u = u + 1
      }

      do {
        u = u - 2
      } while (u < 20)
    })

    val rhs = typeCheck(reify {
      var v = x
      while (v < 20) {
        println(y)
        v = v + 1
      }

      do {
        v = v - 2
      } while (v < 20)
    })

    lhs shouldEqual rhs
  }

  "pattern matching" in {
    val lhs = typeCheck(reify {
      val u = (t, x)
      u match {
        case ((i, j: String), _) => i * 42
      }
    })

    val rhs = typeCheck(reify {
      val v = (t, x)
      v match {
        case ((l, m: String), _) => l * 42
      }
    })

    lhs shouldEqual rhs
  }
}
