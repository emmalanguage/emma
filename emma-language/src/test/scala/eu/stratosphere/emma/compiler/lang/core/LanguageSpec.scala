package eu.stratosphere
package emma.compiler
package lang
package core

import emma.testschema.Marketing._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

/** A spec defining the subset of Scala modeling the LNF language used by the Emma compiler. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  import compiler._
  import universe._
  import Core.Language._
  import Core.valid
  import Validation._

  def satisfy(validator: Validator) =
    be (good) compose { (tree: Tree) =>
      time(validateAs(validator, tree), "validate")
    }

  override def alphaEqTo(tree: Tree) =
    super.alphaEqTo(Owner.at(Owner.enclosing)(tree))

  def `type-check and extract from`(tree: Tree) =
    Type.check(tree) match {
      case Apply(_, args) => args
      case u.Block(stats, _) => stats
    }

  "literals" - {
    // modeled by
    // - `lit` objects in `Core.Language`
    // - `Literal(Constant(value))` nodes in Scala ASTs
    val examples = `type-check and extract from`(reify(
      42, 42L, 3.14, 3.14F, .1e6, 'c', "string"
    ).tree)

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

  "this references" - {
    // modeled by
    // - `this_` objects in `Core.Language`
    // - `This(qualifier)` nodes in Scala ASTs
    val examples = `type-check and extract from`(reify {
      class Unqualified { println(this) }
      class Qualified { println(Qualified.this) }
      class Outer { println(LanguageSpec.this) }
      object Module { println(this) }
      ()
    }.tree).flatMap(_ collect {
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

  "references" - {
    // modeled by
    // - `ref` objects in `Core.Language`
    // - `Ident(sym)` nodes where `sym` is a (free) TermSymbol in Scala ASTs
    val (u, v, w) = (42, 3.14, "string")

    val examples = `type-check and extract from`(reify(
      u, v, w
    ).tree)

    val negatives = `type-check and extract from`(q"""(
      scala.Seq, // package object
      _root_.scala.Predef, // qualified with root
      scala.collection.immutable.List // qualified without root
    )""")

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.ref)
      no (negatives) should satisfy (valid.ref)
    }

    "can be destructed and constructed" in {
      examples foreach { case x@ref(sym) =>
        x shouldBe alphaEqTo (ref(sym))
      }
    }
  }

  "qualified references" - {
    // modeled by
    // - `sel` objects in `Core.Language`
    // - `Select(qual, member)` nodes where `sym` is a (free) TermSymbol in Scala ASTs
    val (u, v, w) = (42, 3.14, "string")

    val examples = `type-check and extract from`(q"""(
      _root_.scala.Predef, // qualified with root
      scala.collection.immutable.List // qualified without root
    )""")

    val negatives = `type-check and extract from`(reify(
      u, v, w, // unqualified refs
      Predef.Set // selection
    ).tree)

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.qref)
      no (negatives) should satisfy (valid.qref)
    }

    "can be destructed and constructed" in {
      examples foreach { case x@qref(target, member) =>
        x shouldBe alphaEqTo (qref(target, member))
      }
    }
  }

  "selections" - {
    // modeled by
    // - `sel` objects in `Core.Language`
    // - `Select(qualifier, name)` nodes in Scala ASTs
    val pair = this.t

    val examples = `type-check and extract from`(reify(
      42.toChar, // literal
      pair._1, // reference
      pair.swap // no-arg method
    ).tree) ++ `type-check and extract from`(q"""(
      scala.Seq, // package object
      _root_.scala.Predef.Set, // qualified with root
      scala.Predef.Map // qualified without root
    )""")

    val negatives = `type-check and extract from`(reify(
      pair.swap._2, // chain
      Seq.fill(10)('!').length, // method call
      new Tuple2(3.14, "pi")._1, // class instantiation
      (pair: (Int, CharSequence))._2 // type ascription
    ).tree) ++ `type-check and extract from`(q"""(
      _root_.scala.Predef, // qualified with root
      scala.collection.immutable.List // qualified without root
    )""")

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.sel)
      no (negatives) should satisfy (valid.sel)
    }

    "can be destructed and constructed" in {
      examples foreach { case x@sel(tgt, member) =>
        x shouldBe alphaEqTo (sel(tgt, member))
      }
    }
  }

  "local method calls" - {
    // modeled by
    // - `app` objects in `Core.Language`
    // - `Apply(method, args)` nodes in Scala ASTs
    val n = 42
    implicit val boolOrd = Ordering.Boolean

    val examples = `type-check and extract from`(reify {
      // definitions
      def sqr(x: Int) = x * x
      def id[A](x: A) = x
      def fill(n: Int)(ch: Char) = new String(Array.fill(n)(ch))
      def greet() = println("Hello, World!")
      def nil[A] = List.empty[A]
      def cmp[A](x: A, y: A)(implicit Ord: Ordering[A]) = Ord.compare(x, y)

      // calls
      sqr(42) // literal
      id(this) // this
      sqr(n) // reference
      fill(n)('!') // multiple parameter lists
      greet() // 0-arg method
      nil[(String, Int)] // type-args only
      cmp(true, false) // implicit args
      ()
    }.tree) collect { case app: Apply => app }

    val negatives = `type-check and extract from`(reify {
      // definitions
      def sqr(x: Long) = x * x
      def id[A](x: A) = x
      def fill(n: Int)(ch: Char) = new String(Array.fill(n)(ch))
      def greet() = println("Hello, World!")
      def nil[A] = List.empty[A]
      def cmp[A](x: A, y: A)(implicit Ord: Ordering[A]) = Ord.compare(x, y)

      // calls
      sqr(42 + n) // method call
      id(new Tuple2(3.14, "pi")) // class instantiation
      sqr(n: Long) // type ascription
      fill(this.x)('!') // selection
      sqr(sqr(4)) // nested
      ()
    }.tree) collect { case app: Apply => app }

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.app)
      no (negatives) should satisfy (valid.app)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@app(method, targs, argss@_*) =>
        x shouldBe alphaEqTo (app(method, targs: _*)(argss: _*))
      }
    }
  }

  "method calls" - {
    // modeled by
    // - `call` objects in `Core.Language`
    // - `Apply(target, args)` nodes in Scala ASTs
    val n = 42
    val list = List(1, 2, 3)
    implicit val pair = 3.14 -> "pi"

    val examples = `type-check and extract from`(reify((
      Predef.println("string"), // literal
      n - 2, // reference
      2 - n, // reference
      this.wait(5), // this
      Seq.fill(n)('!'), // multiple parameter lists
      3.14.toString, // 0-arg method
      scala.collection.Set.empty[(String, Int)], // type-args only
      Predef.implicitly[(Double, String)] // implicit args
    )).tree)

    val negatives = `type-check and extract from`(reify((
      Predef.println(s"n: $n"), // complex argument
      (n / 5) - 2, // complex target
      this.wait(5: Long), // type ascription
      Seq.fill(this.x)('!'), // selection
      list.sum // complex implicit argument
    )).tree)

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.call)
      no (negatives) should satisfy (valid.call)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@call(target, method, targs, argss@_*) =>
        x shouldBe alphaEqTo (call(target, method, targs: _*)(argss: _*))
      }
    }
  }

  "class instantiations" - {
    // modeled by
    // - `inst` objects in `Core.Language`
    // - `Apply(tpt: New, args)` nodes in Scala ASTs

    // Resolves type examples `tpt` occurring in `New(tpt)`
    val fix: Tree => Tree = transform {
      case New(tpt: Tree) => New(Type quote tpt.tpe)
    }

    val services = AdClass.SERVICES
    val examples = `type-check and extract from`(reify(
      new Ad(1, "Uber AD", services),
      new Tuple2(3.14, "pi"), // inferred type-args
      new Array[Int](10), // explicit type-args
      new mutable.ListBuffer[String], // type-args only
      new Object // no-args
    ).tree) map fix

    val negatives = `type-check and extract from`(reify(
      new Tuple2(x + 3.14, "pi"), // method call
      new Array[Int](t._1) // selection
    ).tree) map fix

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.inst)
      no (negatives) should satisfy (valid.inst)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@inst(clazz, targs, argss@_*) =>
        x shouldBe alphaEqTo (inst(clazz, targs: _*)(argss: _*))
      }
    }
  }

  "lambdas" - {
    // modeled by
    // - `lambda` objects in `Core.Language`
    // - `Function(args, body)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify(
      (x: Int, y: Int) => { val sum = x + y; sum },
      "ellipsis".charAt _
    ).tree) flatMap {
      case u.Block(stats, _) => stats
      case tree => tree :: Nil
    }

    val negatives = `type-check and extract from`(reify(
      // body not a let-expr
      (x: Int, y: Int) => x + y,
      (n: Int, x: Int) => {
        val n2 = n * n
        def timesN2(i: Int) = i * n2
        timesN2(x)
      }
    ).tree)

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.lambda)
      no (negatives) should satisfy (valid.lambda)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@lambda(sym, params, body) =>
        x shouldBe alphaEqTo (lambda(params: _*)(body))
      }
    }
  }

  "type ascriptions" - {
    // modeled by
    // - `typed` objects in `Core.Language`
    // - `Typed(expr, tpt)` nodes nodes in Scala ASTs
    val pi = 3.14

    val examples = `type-check and extract from`(reify(
      42: Int, // literal
      pi: Double, // reference
      "string": CharSequence, // upcast
      42: Long // coercion
    ).tree)

    val negatives = `type-check and extract from`(reify(
      x + 42: Int, // method call
      t._2: CharSequence, // selection
      new Tuple2(pi, "3.14"): (Double, CharSequence) // class instantiation
    ).tree)

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.typed)
      no (negatives) should satisfy (valid.typed)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@typed(tree, tpe) =>
        x shouldBe alphaEqTo (typed(tree, tpe))
      }
    }
  }

  "conditionals" - {
    // modeled by
    // - `if_` objects in `Core.Language`
    // - `If(cond, thn, els)` nodes nodes in Scala ASTs
    val x = this.x
    val maybe = false

    val examples = `type-check and extract from`(reify {
      def add(x: Int, y: Int) = x + y
      if (true) "yes" else "no" // literals
      if (maybe) x // references
      if (maybe) add(x, 5) else add(x, -5) // local method calls
      ()
    }.tree) collect { case branch: If => branch }

    val negatives = `type-check and extract from`(reify(
      if (true) x.toString else t._2, // complex branches
      if (t._1 > 42) x // complex condition
    ).tree)

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.if_)
      no (negatives) should satisfy (valid.if_)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@if_(cond, thn, els) =>
        x shouldBe alphaEqTo (if_(cond, thn, els))
      }
    }
  }

  "value definitions" - {
    // modeled by
    // - `val_` objects in `Core.Language`
    // - `ValDef(lhs, rhs)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify {
      val literal = 42
      val `this` = this
      val reference = literal
      val methodCall = reference * 2
      val instance = new Tuple2(methodCall, "string")
      val selection = instance.swap
      val lambda = (x: Int) => { val sqr = x * x; sqr }
      val typed = 3.14: Double
      ()
    }.tree)

    val negatives = `type-check and extract from`(reify {
      val chain = 42.toChar.asDigit
      val methodCall = t._1 * x
      val instance = new Tuple2(chain + 1, methodCall.toString)
      val nested = {
        val half = instance._1 / 2
        half * half
      }
      ()
    }.tree)

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.val_)
      no (negatives) should satisfy (valid.val_)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@val_(lhs, rhs, flags) =>
        x shouldBe alphaEqTo (val_(lhs, rhs, flags))
      }
    }
  }

  "let expressions" - {
    // modeled by
    // - `let` objects in `Core.Language`
    // - `Block(stats, expr)` nodes nodes in Scala ASTs

    val examples = `type-check and extract from`(reify {
      { // vals-only
        val greeting = "Hello, World!"
        println(greeting)
      }
      { // defs-only
        def times2(x: Int) = { val prod = x * 2; prod }
        times2(42)
      }
      { // vals and defs
        val n = 2
        def timesN(x: Int) = { val prod = x * n; prod }
        timesN(42)
      }
      ()
    }.tree)

    val negatives = `type-check and extract from`(reify {
      { // inverted
        def times(n: Int, x: Int) = { val prod = x * n; prod }
        val n = 2
        times(n, 42)
      }
      { // mixed
        val m = 2
        def timesM(x: Int) = { val prod = x * m; prod }
        val n = 3
        def plusN(x: Int) = { val sum = x + n; sum }
        val m42 = timesM(42)
        val n42 = plusN(42)
        val diff = m42 - n42
        println(diff)
      }
      ()
    }.tree)

    assert(examples.nonEmpty, "No spec examples given")
    assert(negatives.nonEmpty, "No negative examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.let)
      no (negatives) should satisfy (valid.let)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@let(vals, defs, expr) =>
        x shouldBe alphaEqTo (let(vals, defs, expr))
      }
    }
  }

  "local method definitions" - {
    // modeled by
    // - `def_` objects in `Core.Language`
    // - `DefDef` nodes nodes in Scala ASTs
    val n = 2

    val examples = `type-check and extract from`(reify {
      // zero-args
      def greet() = {
        val greeting = "Hello, World!"
        println(greeting)
      }
      // pure
      def add(x: Int, y: Int) = {
        val sum = x + y
        sum
      }
      // with closure
      def timesN(x: Int) = {
        val prod = x * n
        prod
      }
      // nested
      def times2N(x: Int) = {
        val n2 = n * 2
        def times(a: Int, b: Int) = {
          val prod = a * b
          prod
        }
        times(n2, x)
      }
      ()
    }.tree)

    val negatives = `type-check and extract from`(reify {
      // no-args
      def e = {
        val builtin = math.E
        builtin
      }
      // multiple arg-lists
      def add(x: Int)(y: Int) = {
        val sum = x + y
        sum
      }
      // body not a let-expr
      def mul(x: Int, y: Int) = x * y
      ()
    }.tree)

    assert(examples.nonEmpty, "No spec examples given")

    "are valid language constructs" in {
      all (examples) should satisfy (valid.def_)
      no (negatives) should satisfy (valid.def_)
    }

    "can be constructed and destructed" in {
      examples foreach { case x@def_(method, flags, params, body) =>
        x shouldBe alphaEqTo (def_(method, flags)(params: _*)(body))
      }
    }
  }
}
