package eu.stratosphere.emma
package compiler
package lang
package core

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `Core.foreach2loop` transformation. */
@RunWith(classOf[JUnitRunner])
class ForeachSpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  val pipeline: Tree => Tree = {
    Type.check(_: Tree)
  } andThen {
    time(Core.foreach2loop(_), "foreach to loop")
  } andThen {
    Owner.at(Owner.enclosing)
  }

  def `type-check and foreach->loop`[T](expr: Expr[T]): Tree =
    pipeline(expr.tree)

  "foreach" - {
    "without closure modification" in {
      val act = `type-check and foreach->loop`(reify {
        for (i <- 1 to 5) println(i)
      })

      val exp = typeCheck(reify {
        for (i <- 1 to 5) println(i)
      })

      act shouldBe alphaEqTo (exp)
    }

    "without argument access" in {
      val act = `type-check and foreach->loop`(reify {
        var x = 42
        for (_ <- 1 to 5) x += 1
      })

      val exp = typeCheck(reify {
        var x = 42; {
          val iter$1 = 1.to(5).toIterator
          var _$1 = null.asInstanceOf[Int]
          while (iter$1.hasNext) {
            _$1 = iter$1.next
            x += 1
          }
        }
      })

      act shouldBe alphaEqTo (exp)
    }

    "with argument access" in {
      val act = `type-check and foreach->loop`(reify {
        var x = 42
        for (i <- 1 to 5) x += i
      })

      val exp = typeCheck(reify {
        var x = 42; {
          val iter$1 = 1.to(5).toIterator
          var i$1 = null.asInstanceOf[Int]
          while (iter$1.hasNext) {
            i$1 = iter$1.next
            x += i$1
          }
        }
      })

      act shouldBe alphaEqTo (exp)
    }
  }
}
