package eu.stratosphere.emma.compiler.ir.core

import eu.stratosphere.emma.api.{CSVInputFormat, DataBag}
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  import compiler.universe._

  def typecheckAndValidate[T](expr: Expr[T]): Boolean = {
    val pipeline = {
      compiler.typeCheck(_: Tree)
    } andThen {
      compiler.Core.validate _
    }

    pipeline(expr.tree)
  }

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
      var u = s"$x is $y"
    }) shouldBe true
  }

  // modeled by `Function(args, body)` nodes
  "anonymous function definitions" - {
    typecheckAndValidate(reify {
      (x: Int, y: Int) => x + y
    }) shouldBe true
  }

  // modeled by `Assign(lhs, rhs)` nodes
  "variable assignment" in {
    typecheckAndValidate(reify {
      var u = "still a ValDef but mutable"
      u = "an updated ValDef"
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

  // modeled by `LabelDef(...)` nodes
  "while loops" in {
    typecheckAndValidate(reify {
      var i = 0
      var r = 0
      while (i < x) {
        i = i + 1
        r = r * i
      }
      i * r
    }) shouldBe true

    typecheckAndValidate(reify {
      var i = 0
      var r = 0
      do {
        i = i + 1
        r = r * i
      } while (i < x)
      i * r
    }) shouldBe true
  }

  // modeled by `Match(selector, cases)` nodes
  "pattern matching" in {
    typecheckAndValidate(reify {
      (1, 2) match {
        case (1, u: Int) => u
        case (2, v) => v
        case _ => x
      }
    }) shouldBe true
  }
}
