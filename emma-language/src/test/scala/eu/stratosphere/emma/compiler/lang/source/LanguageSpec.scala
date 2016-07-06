package eu.stratosphere
package emma.compiler
package lang
package source

import emma.api.DataBag
import emma.testschema.Marketing._

import org.example.foo.{Bar, Baz}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec defining the core fragment of Scala supported by Emma. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  import compiler._
  import universe._
  import Source.Language._
  import Source.valid
  import Validation._

  def satisfy(validator: Validator) =
    be (good) compose { (tree: Tree) =>
      time(validateAs(validator, tree), "validate")
    }

  override def alphaEqTo(tree: Tree) =
    super.alphaEqTo(Owner.at(Owner.enclosing)(tree))

  def `type-check and extract from`[A](expr: Expr[A]) =
    Type.check(expr.tree) match {
      case Apply(_, args) => args
      case u.Block(stats, _) => stats
    }

  // ---------------------------------------------------------------------------
  // atomics
  // ---------------------------------------------------------------------------

  "this" - {
    // modeled by
    // - `this_` objects in `Source.Language`
    // - `This(qual)` nodes in Scala ASTs

    val examples = `type-check and extract from`(reify {
      class A { println(this.toString) }
      class B { println(LanguageSpec.this.x) }
      object C { println(this.hashCode) }
    }).flatMap(_ collect {
      case ths: This => ths
    })

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.this_)
    }

    "can be destructed and constructed" in {
      examples foreach { case x@this_(sym) =>
        x shouldBe alphaEqTo (this_(sym, x.qual.nonEmpty))
      }
    }
  }

  "literals" - {
    // modeled by
    // - `lit` objects in `Source.Language`
    // - `Literal(Constant(value))` nodes in Scala ASTs

    val examples = `type-check and extract from`(reify(42, 4.2, "42"))
    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.lit)
    }

    "can be destructed and constructed" in {
      examples foreach { case x@lit(const) =>
        x shouldBe alphaEqTo (lit(const))
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

    val examples = `type-check and extract from`(reify(u, v, w))
    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.ref)
    }

    "can be destructed and constructed" in {
      examples foreach { case x@ref(sym) =>
        x shouldBe alphaEqTo (ref(sym))
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

    val examples = `type-check and extract from`(reify(t._2, DEFAULT_CLASS))
    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.sel)
    }

    "can be destructed and constructed" in {
      examples foreach { case x@sel(tgt, member) =>
        x shouldBe alphaEqTo (sel(tgt, member))
      }
    }
  }

  "function applications" - {
    // modeled by
    // - `app` objects in `Source.Language`
    // - `Apply(fun, args)` nodes in Scala ASTs

    val examples = `type-check and extract from`(reify(
      x == 42,
      scala.Predef.println(y),
      y.substring(1),
      ((x: Int, y: Int) => x + y) (x, x),
      Seq(x, x),
      DataBag(xs.fetch()),
      List.canBuildFrom[Int],
      DataBag(Seq(1, 2, 3)).sum
    )) map Type.ascription.remove

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.app)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@app(target, targs, argss@_*) =>
        x shouldBe alphaEqTo (app(target, targs: _*)(argss: _*))
      }
    }
  }

  "class instantiation" - {
    // modeled by
    // - `inst` objects in `Source.Language`
    // - `Apply(tpt: New, _)` nodes in Scala ASTs

    // Resolves type examples `tpt` occurring in `New(tpt)`
    val fix: Tree => Tree = preWalk {
      case New(tpt: Tree) =>
        New(Type quote tpt.tpe) // or Type.tree(tpt.tpe)
    }

    val examples = `type-check and extract from`(reify(
      new Ad(1, "Uber AD", AdClass.SERVICES),
      new Baz(x),
      new Bar[Int](x)
    )) map fix

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.inst)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@inst(clazz, targs, argss@_*) =>
        x shouldBe alphaEqTo (inst(clazz, targs: _*)(argss: _*))
      }
    }
  }

  "lambdas" - {
    // modeled by
    // - `lambda` objects in `Source.Language`
    // - `Function(args, body)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify(
      (x: Int, y: Int) => x + y,
      "ellipsis".charAt _
    )) flatMap {
      case u.Block(stats, _) => stats
      case tree => tree :: Nil
    }

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.lambda)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@lambda(sym, params, body) =>
        x shouldBe alphaEqTo (lambda(params: _*)(body))
      }
    }
  }

  "type ascriptions" - {
    // modeled by
    // - `inst` objects in `Source.Language`
    // - `Typed(expr, tpt)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify(
      x: Number,
      t: (Any, String)
    ))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.typed)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@typed(tree, tpe) =>
        x shouldBe alphaEqTo (typed(tree, tpe))
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

    val examples = `type-check and extract from`(reify {
      val u = s"$x is $y"
      val v = 42
    }) map { case vd: ValDef => vd }

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.val_)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@val_(lhs, rhs, flags) =>
        x shouldBe alphaEqTo (val_(lhs, rhs, flags))
      }
    }
  }

  "variable assignment" - {
    // modeled by
    // - `val_` objects in `Source.Language`
    // - `Assign(lhs, rhs)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify {
      var u = "still a ValDef but mutable"
      var w = 42
      u = "an updated ValDef"
      w = w + 1
    }) collect {
      case assign: Assign => assign
    }

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.assign)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@assign(lhs, rhs) =>
        x shouldBe alphaEqTo (assign(lhs, rhs))
      }
    }
  }

  "blocks" - {
    // modeled by
    // - `block` objects in `Source.Language`
    // - `Block(stats, expr)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify(
      { val z = 5; x + z },
      t._2 + "implicit unit": Unit
    )) map Type.ascription.remove

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.block)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@block(stats, expr) =>
        x shouldBe alphaEqTo (block(stats, expr))
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

    val examples = `type-check and extract from`(reify(
      if (x == 42) x else x / 42,
      if (x < 42) "only one branch"
    ))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.branch)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@branch(cond, thn, els) =>
        x shouldBe alphaEqTo (branch(cond, thn, els))
      }
    }
  }

  "while-do loops" - {
    // modeled by
    // - `whiledo` objects in `Source.Language`
    // - `LabelDef(...)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify {
      var r = 0
      var i = 0
      while (i < x) {
        i = i + 1
        r = r * i
      }
      i * r
    }) collect {
      case loop: LabelDef => loop
    }

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.whiledo)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@whiledo(cond, body) =>
        x shouldBe alphaEqTo (whiledo(cond, body))
      }
    }
  }

  "do-while loops" - {
    // modeled by
    // - `dowhile` objects in `Source.Language`
    // - `LabelDef(...)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify {
      var i = 0
      var r = 0
      do {
        i = i + 1
        r = r * i
      } while (i < x)
      i * r
    }) collect {
      case loop: LabelDef => loop
    }

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.dowhile)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@dowhile(cond, body) =>
        x shouldBe alphaEqTo (dowhile(cond, body))
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

    val examples = `type-check and extract from`(reify(
      ((1, 2): Any) match {
        case (x: Int, _) => x
        case Ad(id, name, _) => id
        case Click(adID, userID, time) => adID
        case _ => 42
      },
      "binding" match {
        case s@(_: String) => s + t._2
      }
    ))

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.match_)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@match_(selector, cases) =>
        x shouldBe alphaEqTo (match_(selector, cases map {
          case case_(pat, guard, body) => case_(pat, guard, body)
        }))
      }
    }
  }
}
