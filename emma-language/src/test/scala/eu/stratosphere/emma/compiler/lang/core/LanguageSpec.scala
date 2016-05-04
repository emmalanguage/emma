package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import eu.stratosphere.emma.testschema.Marketing._
import org.example.foo.{Bar, Baz}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the subset of Scala modeling the LNF language used by the Emma compiler. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  import compiler._
  import universe._
  import Core.Language._

  def typeCheckAndValidate[T]: Expr[T] => Boolean = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_: Tree)
  } andThen {
    time(Core.validate(_), "validate")
  }

  def validate: Tree => Boolean = {
    time(Core.validate(_), "validate")
  }

  val repair: Tree => Tree =
    Owner.at(Owner.enclosing)

  // ---------------------------------------------------------------------------
  // atomics
  // ---------------------------------------------------------------------------

  "literals" - {
    // modeled by
    // - `lit` objects in `Core.Language`
    // - `Literal(Constant(value))` nodes in Scala ASTs

    val examples = List(
      typeCheck(reify(42)),
      typeCheck(reify(4.2)),
      typeCheck(reify("42")))
      .map(_ find {
        case lit(_) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be destructed and constructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case lit(const) =>
          repair(lit(const)) shouldBe alphaEqTo(example)
      }
    }
  }

  "references" - {
    // modeled by
    // - `ref` objects in `Core.Language`
    // - `Ident(name)` nodes where `sym` is a (free) TermSymbol in Scala ASTs

    val u = 42
    val v = 4.2
    val w = "42"

    val examples = List(
      typeCheck(reify(u)),
      typeCheck(reify(v)),
      typeCheck(reify(w)))
      .map(_ find {
        case ref(_) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be destructed and constructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case ref(sym) =>
          repair(ref(sym)) shouldBe alphaEqTo(example)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // terms
  // ---------------------------------------------------------------------------

  "selections" - {
    // modeled by
    // - `sel` objects in `Core.Language`
    // - `Select(qualifier, name)` nodes in Scala ASTs

    val t = this.t

    val examples = List(
      typeCheck(reify(t._2)),
      typeCheck(reify(DEFAULT_CLASS)))
      .map(_ find {
        case sel(_, _) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be destructed and constructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case sel(tgt, member) =>
          repair(sel(tgt, member)) shouldBe alphaEqTo(example)
      }
    }
  }

  "function applications" - {
    // modeled by
    // - `app` objects in `Core.Language`
    // - `Apply(fun, args)` nodes in Scala ASTs

    val x = this.x

    val examples = List(
      typeCheck(reify {
        def max(x: Int, y: Int): Int = Math.max(x, y)
        max(42, x)
      }))
      .map(_ find {
        case app(_, _, _*) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be destructed and constructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case app(fn, targs, argss@_*) =>
          repair(app(fn, targs: _*)(argss: _*)) shouldBe alphaEqTo(example)
      }
    }
  }

  "method calls" - {
    // modeled by
    // - `call` objects in `Core.Language`
    // - `Apply(Select(id, TermSymbol), args)` nodes in Scala ASTs

    val x = this.x
    val y = this.y
    val xs = this.xs
    val ys = this.ys.fetch()

    val examples = List(
      typeCheck(reify {
        x == 42
      }),
      typeCheck(reify {
        scala.Predef.println(y)
      }),
      typeCheck(reify {
        y.substring(1)
      }),
      typeCheck(reify {
        val f = (x: Int, y: Int) => Math.max(x, y)
        f(42, x)
      }),
      typeCheck(reify {
        Seq(x, x)
      }),
      typeCheck(reify {
        xs.fetch()
      }),
      typeCheck(reify {
        DataBag(ys)
      }))
      .map(_ find {
        case call(_, _, _, _*) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be destructed and constructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case call(target, method, targs, argss@_*) =>
          repair(repair(call(target, method, targs: _*)(argss: _*))) shouldBe alphaEqTo(example)
      }
    }
  }

  "class instantiations" - {
    // modeled by
    // - `inst` objects in `Core.Language`
    // - `Apply(tpt: New, _)` nodes in Scala ASTs

    val x = this.x
    val adClass = AdClass.SERVICES
    implicit val intNumeric = Numeric.IntIsIntegral

    val examples = List(
      typeCheck(reify(new Ad(1, "Uber AD", adClass))),
      typeCheck(reify(new Baz(x))),
      typeCheck(reify(new Bar[Int](x))))
      .map(_ find {
        case inst(_, _, _*) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")


    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be constructed and destructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case inst(clazz, targs, argss@_*) =>
          repair(inst(clazz, targs: _*)(argss: _*)) shouldBe alphaEqTo(example)
      }
    }
  }

  "lambdas" - {
    // modeled by
    // - `lambda` objects in `Core.Language`
    // - `Function(args, body)` nodes nodes in Scala ASTs

    val examples = List(
      typeCheck(reify((x: Int, y: Int) => { val s = x + y; s * 15 })))
      .map(_ find {
        case lambda(_, _, _) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be constructed and destructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case lambda(sym, params, body) =>
          repair(lambda(params: _*)(body)) shouldBe alphaEqTo(example)
      }
    }
  }

  "type ascriptions" - {
    // modeled by
    // - `inst` objects in `Core.Language`
    // - `Typed(expr, tpt)` nodes nodes in Scala ASTs

    val x = this.x
    val t = this.t

    val examples = List(
      typeCheck(reify(x: Int)),
      typeCheck(reify(t: (Any, String))))
      .map(_ find {
        case typed(_, _) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be constructed and destructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case typed(tree, tpe) =>
          repair(typed(tree, tpe)) shouldBe alphaEqTo(example)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // let expressions
  // ---------------------------------------------------------------------------

  "value and variable definitions" - {
    // modeled by
    // - `val_` objects in `Core.Language`
    // - `ValDef(lhs, rhs)` nodes nodes in Scala ASTs

    val x = this.x
    val y = this.y

    val examples = List(
      typeCheck(reify {
        val u = s"$x is $y"
      }),
      typeCheck(reify {
        val u = 42
      }))
      .map(_ find {
        case val_(_, _, _) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be constructed and destructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case val_(lhs, rhs, flags) =>
          repair(val_(lhs, rhs, flags)) shouldBe alphaEqTo(example)
      }
    }
  }
  
  "let expressions" - {
    // modeled by
    // - `let_` objects in `Core.Language`
    // - `Block(stats, expr)` nodes nodes in Scala ASTs

    val x = this.x
    val y = this.y

    val examples = List(
      typeCheck(reify {
        val u = s"$x is $y"
        val z = u(15)
        z == 'x'
      }),
      typeCheck(reify {
        val q = "The meaning of life, the universe and everything?"
        val a = 42
        (q, a)
      }),
      typeCheck(reify {
        val q = "Factorial of the answer for the meaning of life, the universe and everything?"
        val a = 42
        def fac(n: Int): Int =
          if (n > 0) n * fac(n-1)
          else 1
        fac(a)
      }))
      .map(_ find {
        case let_(_, _, _) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be constructed and destructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case let_(vals, defs, expr) =>
          repair(let_(vals, defs, expr)) shouldBe alphaEqTo(example)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // control flow
  // ---------------------------------------------------------------------------

  "conditionals" - {
    // modeled by
    // - `block` objects in `Source.Language`
    // - `If(cond, thenp, elsep)` nodes nodes in Scala ASTs

    val expCnd = this.x == 42
    val expThn = this.x
    val expEls = this.x - 42

    val examples = List(
      typeCheck(reify(if (expCnd) expThn else expEls)))
      .map(_ find {
        case if_(_, _, _) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be constructed and destructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case if_(cond, thn, els) =>
          repair(if_(cond, thn, els)) shouldBe alphaEqTo(example)
      }
    }
  }

  "simple functions" - {
    // modeled by
    // - `block` objects in `Source.Language`
    // - `If(cond, thenp, elsep)` nodes nodes in Scala ASTs

    val cnd = this.x == 42
    val thn = this.x
    val els = this.x - 42

    val examples = List(
      typeCheck(reify {
        def max(x: Int, y: Int): Int = {
          val m = Math.max(x, y)
          Math.max(m, 0)
        }
      }),
      typeCheck(reify {
        def fac(n: Int): Int = {
          val more = n > 0
          if (more) n * fac(n-1)
          else 1
        }
      }))
      .map(_ find {
        case def_(_, _, _, _) => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")
    assert(examples.forall(_.isDefined), "Not all code snippets define an expected match")

    "are valid language constructs" in {
      for (Some(example) <- examples)
        validate(example) shouldBe true
    }

    "can be constructed and destructed" in {
      for ((Some(example), i) <- examples.zipWithIndex) example match {
        case def_(sym, flags, params, body) =>
          repair(def_(sym, flags)(params: _*)(body)) shouldBe alphaEqTo(example)
      }
    }
  }
}
