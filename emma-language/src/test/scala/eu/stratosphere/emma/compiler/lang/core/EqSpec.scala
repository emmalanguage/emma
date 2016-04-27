package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for alpha-equivalence on LNF rees. */
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

  "loops" in {
    val lhs = typeCheck(reify {
      def b$00(): Unit = {
        val i$1 = 0
        val r$1 = 0
        def b$01$while(i: Int, r: Int): Int = {
          def b$01$body(): Int = {
            val i$2 = i + 1
            val r$2 = r * i$2
            b$01$while(i$2, r$2)
          }
          def b$02(i: Int, r: Int): Int = {
            i * r
          }
          if (i < x) b$01$body()
          else b$02(i, r)
        }
        b$01$while(i$1, r$1)
      }
      b$00()
    })

    val rhs = typeCheck(reify {
      def x$00(): Unit = {
        val j$1 = 0
        val k$1 = 0
        def x$01$while(j: Int, k: Int): Int = {
          def x$01$body(): Int = {
            val j$2 = j + 1
            val k$2 = k * j$2
            x$01$while(j$2, k$2)
          }
          def x$02(i: Int, j: Int): Int = {
            i * j
          }
          if (j < x) x$01$body()
          else x$02(j, k)
        }
        x$01$while(j$1, k$1)
      }
      x$00()
    })

    lhs shouldEqual rhs
  }
}
