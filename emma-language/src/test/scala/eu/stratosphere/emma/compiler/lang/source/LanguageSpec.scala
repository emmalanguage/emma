package eu.stratosphere.emma.compiler.lang.source

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import eu.stratosphere.emma.testschema.Marketing._
import org.example.foo.{Bar, Baz}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec with TreeEquality {

  import compiler._
  import universe._
  import Source.Language._

  def validate: Tree => Boolean = {
    time(Source.validate(_), "validate")
  }

  val repair: Tree => Tree =
    Owner.at(Owner.enclosing)

  // ---------------------------------------------------------------------------
  // atomics
  // ---------------------------------------------------------------------------

  "literals" - {
    // modeled by
    // - `lit` objects in `Source.Language`
    // - `Literal(Constant(value))` nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify(42)),
      typeCheck(reify(4.2)),
      typeCheck(reify("42")))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (t <- examples)
        validate(t) shouldBe true
    }

    "can be destructed and constructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (lit(const), i) =>
          examples(i) shouldEqual repair(lit(const))
      }
    }
  }

  "references" - {
    // modeled by
    // - `ref` objects in `Source.Language`
    // - `Ident(name)` nodes where `sym` is a (free) TermSymbol in Scala ASTs

    val u = 42
    val v = 4.2
    val w = "42"

    val examples: List[Tree] = List(
      typeCheck(reify(u)),
      typeCheck(reify(v)),
      typeCheck(reify(w)))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (t <- examples)
        validate(t) shouldBe true
    }

    "can be destructed and constructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (ref(sym), i) =>
          examples(i) shouldEqual repair(ref(sym))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // terms
  // ---------------------------------------------------------------------------

  "selections" - {
    // modeled by
    // - `sel` objects in `Source.Language`
    // - `Select(qualifier, name)` nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify(t._2)),
      typeCheck(reify(DEFAULT_CLASS)))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (t <- examples)
        validate(t) shouldBe true
    }

    "can be destructed and constructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (sel(tgt, member), i) =>
          examples(i) shouldEqual repair(sel(tgt, member))
      }
    }
  }

  "function applications" - {
    // modeled by
    // - `app` objects in `Source.Language`
    // - `Apply(fun, args)` nodes in Scala ASTs

    val examples: List[Tree] = List(
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
        ((x: Int, y: Int) => x + y) (x, x)
      }),
      typeCheck(reify {
        Seq(x, x)
      }),
      typeCheck(reify {
        DataBag(xs.fetch())
      }))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (app(fn, targs, argss), i) =>
          examples(i) shouldEqual repair(app(fn, targs: _*)(argss))
      }
    }
  }

  "class instantiation" - {
    // modeled by
    // - `inst` objects in `Source.Language`
    // - `Apply(tpt: New, _)` nodes in Scala ASTs

    // Resolves type examples `tpt` occurring in `New(tpt)`
    def fix: Tree => Tree = preWalk {
      case New(tpt: Tree) => New(Type.quote(tpt.tpe)) // or Type.tree(tpt.tpe)
    }

    val examples: List[Tree] = List(
      (typeCheck andThen fix) (reify(new Ad(1, "Uber AD", AdClass.SERVICES))),
      (typeCheck andThen fix) (reify(new Baz(x))),
      (typeCheck andThen fix) (reify(new Bar[Int](x))))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (inst(clazz, targs, argss@_*), i) =>
          examples(i) shouldEqual repair(inst(clazz, targs: _*)(argss: _*))
      }
    }
  }

  "lambdas" - {
    // modeled by
    // - `lambda` objects in `Source.Language`
    // - `Function(args, body)` nodes nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify((x: Int, y: Int) => x + y)))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (lambda(sym, params, body), i) =>
          examples(i) shouldEqual repair(lambda(params: _*)(body))
      }
    }
  }

  "type ascriptions" - {
    // modeled by
    // - `inst` objects in `Source.Language`
    // - `Typed(expr, tpt)` nodes nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify(x: Number)),
      typeCheck(reify(t: (Any, String))))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (typed(tree, tpe), i) =>
          examples(i) shouldEqual repair(typed(tree, tpe))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // state
  // ---------------------------------------------------------------------------

  "value and variable definitions" - {
    // modeled by
    // - `val_` objects in `Source.Language`
    // - `ValDef(lhs, rhs)` nodes nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify {
        val u = s"$x is $y"
      }),
      typeCheck(reify {
        val u = 42
      }))
      .collect {
        case Block((vd: ValDef) :: Nil, _) => vd
      }

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (val_(lhs, rhs, flags), i) =>
          examples(i) shouldEqual repair(val_(lhs, rhs, flags))
      }
    }
  }

  "variable assignment" - {
    // modeled by
    // - `val_` objects in `Source.Language`
    // - `Assign(lhs, rhs)` nodes nodes in Scala ASTs

    var u = "still a ValDef but mutable"
    var w = 42
    val examples: List[Tree] = List(
      typeCheck(reify {
        u = "an updated ValDef"
      }),
      typeCheck(reify {
        w = w + 1
      }))
      .flatMap(_ find {
        case ass: Assign => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (assign(lhs, rhs), i) =>
          examples(i) shouldEqual repair(assign(lhs, rhs))
      }
    }
  }

  "blocks" - {
    // modeled by
    // - `block` objects in `Source.Language`
    // - `Block(stats, expr)` nodes nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify {
        val z = 5
        x + z
      }))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (block(stats, expr), i) =>
          examples(i) shouldEqual repair(block(stats, expr))
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

    val examples: List[Tree] = List(
      typeCheck(reify(if (x == 42) x else x / 42)))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (branch(cond, thn, els), i) =>
          examples(i) shouldEqual repair(branch(cond, thn, els))
      }
    }
  }

  "while-do loops" - {
    // modeled by
    // - `whiledo` objects in `Source.Language`
    // - `LabelDef(...)` nodes nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify {
        var r = 0
        var i = 0
        while (i < x) {
          i = i + 1
          r = r * i
        }
        i * r
      }))
      .flatMap(_ find {
        case ldef: LabelDef => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (whiledo(cond, body), i) =>
          examples(i) shouldEqual repair(whiledo(cond, body))
      }
    }
  }

  "do-while loops" - {
    // modeled by
    // - `dowhile` objects in `Source.Language`
    // - `LabelDef(...)` nodes nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify {
        var i = 0
        var r = 0
        do {
          i = i + 1
          r = r * i
        } while (i < x)
        i * r
      }))
      .flatMap(_ find {
        case ldef: LabelDef => true
        case _ => false
      })

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (dowhile(cond, body), i) =>
          examples(i) shouldEqual repair(dowhile(cond, body))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // pattern matching
  // ---------------------------------------------------------------------------

  "pattern matches" - {
    // modeled by
    // - `mat` objects in `Source.Language`
    // - `Match(selector, cases)` nodes nodes in Scala ASTs

    val examples: List[Tree] = List(
      typeCheck(reify {
        ((1, 2): Any) match {
          case (x: Int, _) => x
          case Ad(id, name, _) => id
          case Click(adID, userID, time) => adID
          case _ => 42
        }
      }))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      for (x <- examples)
        validate(x) shouldBe true
    }

    "can be constructed and destructed" in {
      for (t <- examples.zipWithIndex) t match {
        case (match_(selector, cases), i) =>
          examples(i) shouldEqual repair(match_(selector, cases map {
            case cs@case_(pat, guard, body) => case_(pat, guard, body)
          }))
      }
    }
  }
}
