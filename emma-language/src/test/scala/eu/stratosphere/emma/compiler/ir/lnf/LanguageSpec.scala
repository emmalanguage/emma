package eu.stratosphere.emma.compiler.ir.lnf

import eu.stratosphere.emma.api.{CSVInputFormat, DataBag}
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.annotation.tailrec

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  import compiler.universe._

  def typecheckAndValidate[T](expr: Expr[T]): Boolean = {
    compiler.LNF.validate(compiler.typeCheck(expr.tree))
  }

  // common value definitions used below
  val x = 42
  val y = "The answer to life, the universe and everything"
  val t = (x, y)
  val xs = DataBag(Seq(1, 2, 3))
  val ys = DataBag(Seq(1, 2, 3))

  // modeled by `Literal(Constant(value))` nodes
  "literals" in {
    typecheckAndValidate(reify {
      42
    }) shouldBe true

    typecheckAndValidate(reify {
      4.2
    }) shouldBe true

    typecheckAndValidate(reify {
      "42"
    }) shouldBe true
  }

  // modeled by `Ident(name)` where `sym` is a "free term symbol"
  "identifiers" in {
    val u = 42
    typecheckAndValidate(reify {
      u
    }) shouldBe true
  }

  // modeled by `Typed(expr, tpt)` nodes
  "type ascriptions" in {
    typecheckAndValidate(reify {
      42: Int
    }) shouldBe true
  }

  // modeled by `Select(qualifier, name)` nodes
  "field selections" in {
    typecheckAndValidate(reify {
      t._2
    }) shouldBe true

    typecheckAndValidate(reify {
      scala.Int.MaxValue
    }) shouldBe true
  }

  // modeled by `Block(stats, expr)` nodes
  "blocks" in {
    typecheckAndValidate(reify {
      val z = 5
      x + z
    }) shouldBe true
  }

  // modeled by `ValDef(lhs, rhs)` nodes
  "value and variable definitions" in {
    typecheckAndValidate(reify {
      val u = 42
    }) shouldBe true

    typecheckAndValidate(reify {
      val u = s"$x is $y"
    }) shouldBe true
  }

  // modeled by `Function(args, body)` nodes
  "anonymous function definitions" - {
    typecheckAndValidate(reify {
      (x: Int, y: Int) => x + y
    }) shouldBe true
  }

  // modeled by `TypeApply(fun, args)` nodes
  "type applications" in {
    typecheckAndValidate(reify {
      Seq.empty[Int]
    }) shouldBe true
  }

  // modeled by `Apply(fun, args)` nodes
  "function applications" in {
    typecheckAndValidate(reify {
      x == 42
    }) shouldBe true

    typecheckAndValidate(reify {
      x + 4.2
    }) shouldBe true

    typecheckAndValidate(reify {
      scala.Predef.println(y)
    }) shouldBe true

    typecheckAndValidate(reify {
      y.substring(1)
    }) shouldBe true

    typecheckAndValidate(reify {
      ((x: Int, y: Int) => x + y) (x, x)
    }) shouldBe true

    typecheckAndValidate(reify {
      Seq(x, x)
    }) shouldBe true

    typecheckAndValidate(reify {
      val zs: DataBag[Int] = DataBag(xs.fetch())
    }) shouldBe true
  }

  // modeled by `New` nodes
  "class instantiation" in {
    typecheckAndValidate(reify {
      new Tuple2("route", 66)
    }) shouldBe true

    typecheckAndValidate(reify {
      new CSVInputFormat[(Int, String)]
    }) shouldBe true
  }

  // modeled by `If(cond, thenp, elsep)` nodes
  "conditionals" in {
    typecheckAndValidate(reify {
      if (x == 42) x else x / 42
    }) shouldBe true
  }

  // modeled direct-style by `DefDef` call chains
  "while loops" in {
    typecheckAndValidate(reify {
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

    typecheckAndValidate(reify {
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
