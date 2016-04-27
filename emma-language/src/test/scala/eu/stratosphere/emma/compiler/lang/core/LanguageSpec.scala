package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.api.{CSVInputFormat, DataBag}
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the subset of Scala modeling the LNF language used by the Emma compiler. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  def typeCheckAndValidate[T]: Expr[T] => Boolean = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_: Tree)
  } andThen {
    time(Core.validate(_), "validate")
  }

  // modeled by `Literal(Constant(value))` nodes
  "literals" in {
    typeCheckAndValidate(reify {
      42
    }) shouldBe true

    typeCheckAndValidate(reify {
      4.2
    }) shouldBe true

    typeCheckAndValidate(reify {
      "42"
    }) shouldBe true
  }

  // modeled by `Ident(name)` where `sym` is a "free term symbol"
  "identifiers" in {
    val u = 42
    typeCheckAndValidate(reify {
      u
    }) shouldBe true
  }

  // modeled by `Typed(expr, tpt)` nodes
  "type ascriptions" in {
    typeCheckAndValidate(reify {
      42: Int
    }) shouldBe true
  }

  // modeled by `Select(qualifier, name)` nodes
  "field selections" in {
    typeCheckAndValidate(reify {
      t._2
    }) shouldBe true

    typeCheckAndValidate(reify {
      scala.Int.MaxValue
    }) shouldBe true
  }

  // modeled by `Block(stats, expr)` nodes
  "blocks" in {
    typeCheckAndValidate(reify {
      val z = 5
      x + z
    }) shouldBe true
  }

  // modeled by `ValDef(lhs, rhs)` nodes
  "value and variable definitions" in {
    typeCheckAndValidate(reify {
      val u = 42
    }) shouldBe true

    typeCheckAndValidate(reify {
      val u = s"$x is $y"
    }) shouldBe true
  }

  // modeled by `Function(args, body)` nodes
  "anonymous function definitions" - {
    typeCheckAndValidate(reify {
      (x: Int, y: Int) => x + y
    }) shouldBe true
  }

  // modeled by `TypeApply(fun, args)` nodes
  "type applications" in {
    typeCheckAndValidate(reify {
      Seq.empty[Int]
    }) shouldBe true
  }

  // modeled by `Apply(fun, args)` nodes
  "function applications" in {
    typeCheckAndValidate(reify {
      x == 42
    }) shouldBe true

    typeCheckAndValidate(reify {
      x + 4.2
    }) shouldBe true

    typeCheckAndValidate(reify {
      scala.Predef.println(y)
    }) shouldBe true

    typeCheckAndValidate(reify {
      y.substring(1)
    }) shouldBe true

    typeCheckAndValidate(reify {
      ((x: Int, y: Int) => x + y) (x, x)
    }) shouldBe true

    typeCheckAndValidate(reify {
      Seq(x, x)
    }) shouldBe true

    typeCheckAndValidate(reify {
      val zs: DataBag[Int] = DataBag(xs.fetch())
    }) shouldBe true
  }

  // modeled by `New` nodes
  "class instantiation" in {
    typeCheckAndValidate(reify {
      new Tuple2("route", 66)
    }) shouldBe true

    typeCheckAndValidate(reify {
      new CSVInputFormat[(Int, String)]
    }) shouldBe true
  }

  // modeled by `If(cond, thenp, elsep)` nodes
  "conditionals" in {
    typeCheckAndValidate(reify {
      if (x == 42) x else x / 42
    }) shouldBe true
  }

  // modeled direct-style by `DefDef` call chains
  "while loops" in {
    typeCheckAndValidate(reify {
      def b$00(): Unit = {
        val i$1 = 0
        val r$1 = 0
        def b$01$while(i: Int, r: Int): Int = {
          def b$01$body(i: Int, r: Int): Int = {
            val i$2 = i + 1
            val r$2 = r * i$2
            b$01$while(i$2, r$2)
          }
          def b$02(i: Int, r: Int): Int = {
            i * r
          }
          if (i < x) b$01$body(i, r)
          else b$02(i, r)
        }
        b$01$while(i$1, r$1)
      }
      b$00()
    }) shouldBe true

    typeCheckAndValidate(reify {
      def b$00(): Unit = {
        val i$1 = 0
        val r$1 = 0
        def b$01$dowhile(i: Int, r: Int): Int = {
          val i$2 = i + 1
          val r$2 = r * i$2
          def b$02(i: Int, r: Int): Int = {
            i * r
          }
          if (i < x) b$01$dowhile(i$2, r$2)
          else b$02(i, r)
        }
        b$01$dowhile(i$1, r$1)
      }
      b$00()
    }) shouldBe true
  }
}
