package eu.stratosphere.emma.compiler.ir.lnf

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * A spec for alpha-equivalence on trees.
 */
@RunWith(classOf[JUnitRunner])
class EquivalenceSpec extends BaseCompilerSpec {

  import scala.reflect.runtime.universe._

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

    compiler.LNF.eq(t1, t2) shouldBe true
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

    compiler.LNF.eq(t1, t2) shouldBe true
  }

  "loops" in {
    val t1 = typeCheck(reify {
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

    val t2 = typeCheck(reify {
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

    compiler.LNF.eq(t1, t2) shouldBe true
  }
}
