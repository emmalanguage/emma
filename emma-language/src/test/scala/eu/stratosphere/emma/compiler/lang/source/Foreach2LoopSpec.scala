package eu.stratosphere.emma.compiler.lang.source

import eu.stratosphere.emma.compiler.BaseCompilerSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `Core.foreach2loop` transformation. */
@RunWith(classOf[JUnitRunner])
class Foreach2LoopSpec extends BaseCompilerSpec {

  import compiler._

  val foreach2loopPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true) {
      tree => time(Foreach2Loop.transform(tree), "foreach -> loop")
    }.compose(_.tree)

  val idPipeline: u.Expr[Any] => u.Tree =
    compiler.identity(typeCheck = true)
      .compose(_.tree)

  "foreach" - {
    "without closure modification" in {
      val act = foreach2loopPipeline(u.reify {
        for (i <- 1 to 5) println(i)
      })

      val exp = idPipeline(u.reify {
        for (i <- 1 to 5) println(i)
      })

      act shouldBe alphaEqTo (exp)
    }

    "without argument access" in {
      val act = foreach2loopPipeline(u.reify {
        var x = 42
        for (_ <- 1 to 5) x += 1
      })

      val exp = idPipeline(u.reify {
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
      val act = foreach2loopPipeline(u.reify {
        var x = 42
        for (i <- 1 to 5) x += i
      })

      val exp = idPipeline(u.reify {
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

    "with monadic filter" in {
      val act = foreach2loopPipeline(u.reify {
        var x = 42
        for (i <- 1 to 10 if i % 2 == 0) x += i
      })

      val exp = idPipeline(u.reify {
        var x = 42; {
          val iter$1 = 1.to(10)
            .withFilter(_ % 2 == 0)
            .map(x => x).toIterator

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
