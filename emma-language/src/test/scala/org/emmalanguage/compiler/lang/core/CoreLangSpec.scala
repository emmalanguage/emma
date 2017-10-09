/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler.lang.core

import api._
import compiler.BaseCompilerSpec
import test.schema.Marketing._

import scala.collection.mutable

/** A spec defining the subset of Scala modeling the LNF language used by the Emma compiler. */
class CoreLangSpec extends BaseCompilerSpec {

  import compiler._
  import Validation._
  import Core.valid
  import Core.{Lang => core}
  import u.reify

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /** Example pre-processing pipeline. */
  lazy val pipeline =
    TreeTransform("CoreLangSpec.pipeline/compiler.typecheck", { compiler.typeCheck(_: u.Tree) })
    .andThen(compiler.fixSymbolTypes)
    .andThen(compiler.unQualifyStatics)
    .andThen(compiler.normalizeStatements)

  /** Extracts examples from a reified expression. */
  def extractFrom[A](expr: u.Expr[A]) =
  pipeline(expr.tree) match {
    case u.Apply(_, args) => args
    case u.Block(stats, _) => stats
  }

  /** Tree [[Validator]] matcher. */
  def satisfy(validator: Validator) =
    be (good) compose { (tree: u.Tree) =>
      time(validateAs(validator, tree), "validate")
    }

  override def alphaEqTo[T <: u.Tree](tree: T) =
    super.alphaEqTo(api.Owner.atEncl(tree))

  // ---------------------------------------------------------------------------
  // Atomics
  // ---------------------------------------------------------------------------

  "Literals" - {
    // modeled by
    // - `Lit` objects in `Core.Lang`
    // - `Literal(Constant(value))` nodes in Scala ASTs
    val examples = extractFrom(reify(42, 42L, 3.14, 3.14F, .1e6, 'c', "string"))
    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Lit)
    }

    "can be destructed and constructed" in {
      examples foreach { case x @ core.Lit(const) =>
        x shouldBe alphaEqTo (core.Lit(const))
      }
    }
  }

  "References" - {
    // modeled by
    // - `Ref` objects in `Core.Lang`
    // - `Ident(sym)` nodes where `sym` is a (free) TermSymbol in Scala ASTs
    val (z, v, w) = (42, 3.14, "string")

    val examples = extractFrom(reify(
      z, v, w, mutable.Seq, util.Monoids
    ))

    val negatives = extractFrom(reify {
      var x = 42
      var y = 4.2
      var z = "42"
      x -= 1
      y *= 2
      z += '3'
      Seq(x, y, z)
      ()
    }).last.children.tail

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Ref)
      no (negatives) should satisfy (valid.Ref)
    }

    "can be destructed and constructed" in {
      examples foreach { case x @ core.Ref(target) =>
        x shouldBe alphaEqTo (core.Ref(target))
      }
    }
  }

  "This references" - {
    // modeled by
    // - `This` objects in `Core.Lang`
    // - `This(qualifier)` nodes in Scala ASTs
    val examples = extractFrom(reify {
      class Unqualified { println(this) }
      class Qualified { println(Qualified.this) }
      class Outer { println(CoreLangSpec.this) }
      object Module { println(this) }
      Module: AnyRef
    }).map(_.collect {
      case ths: u.This => ths
    }.head)

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.This)
    }

    "can be destructed and constructed" in {
      examples foreach { case x @ core.This(qual) =>
        x shouldBe alphaEqTo (core.This(qual))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Values
  // ---------------------------------------------------------------------------

  "Value definitions" - {
    // modeled by
    // - `ValDef` objects in `Core.Lang`
    // - `ValDef(lhs, rhs)` nodes nodes in Scala ASTs

    //noinspection RedundantNewCaseClass
    val examples = extractFrom(reify {
      val literal = 42
      val `this` = this
      val reference = literal
      val methodCall = reference * 2
      val instance = new Tuple2(methodCall, "string")
      val selection = instance.swap
      val lambda = (x: Int) => { val sqr = x * x; sqr }
      val typed = 3.14: Double
      (`this`, selection, lambda, typed)
    })

    //noinspection RedundantNewCaseClass
    val negatives = extractFrom(reify {
      val chain = 42.toChar.asDigit
      val methodCall = t._1 * x
      val instance = new Tuple2(chain + 1, methodCall.toString)
      val nested = {
        val half = instance._1 / 2
        half * half
      }
      nested
    })

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.ValDef)
      no (negatives) should satisfy (valid.ValDef)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.ValDef(lhs, rhs) =>
        x shouldBe alphaEqTo (core.ValDef(lhs, rhs))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Methods
  // ---------------------------------------------------------------------------

  "Local method calls" - {
    // modeled by
    // - `DefCall` objects in `Core.Lang`
    // - `Apply(method, args)` nodes in Scala ASTs
    val n = 42
    implicit val boolOrd = Ordering.Boolean

    val examples = extractFrom(reify {
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
    }).collect {
      case call: u.Apply => call
    }

    val negatives = extractFrom(reify {
      // definitions
      def sqr(x: Long) = x * x
      def id[A](x: A) = x
      def fill(n: Int)(ch: Char) = new String(Array.fill(n)(ch))

      // calls
      sqr(42 + n) // method call
      id(new String("pi")) // class instantiation
      sqr(n: Long) // type ascription
      fill(this.x)('!') // selection
      sqr(sqr(4)) // nested
      ()
    }).collect {
      case call: u.Apply => call
    }

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.DefCall)
      no (negatives) should satisfy (valid.DefCall)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.DefCall(None, method, targs, argss) =>
        x shouldBe alphaEqTo (core.DefCall(None, method, targs, argss))
      }
    }
  }

  "Method calls" - {
    // modeled by
    // - `DefCall` objects in `Core.Lang`
    // - `Apply(target, args)` nodes in Scala ASTs
    val n = 42
    implicit val pair = 3.14 -> "pi"

    val examples = extractFrom(reify((
      Predef.println("string"), // literal
      n - 2, // reference
      2 - n, // reference
      this.wait(5), // this
      Seq.fill(n)('!'), // multiple parameter lists
      3.14.toString, // 0-arg method
      scala.collection.Set.empty[(String, Int)], // type-args only
      Predef.implicitly[(Double, String)] // implicit args
    )))

    val negatives = extractFrom(reify((
      Predef.println(s"n: $n"), // complex argument
      (n / 5) - 2, // complex target
      this.wait(5: Long), // type ascription
      Seq.fill(this.x)('!') // selection
    )))

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.DefCall)
      no (negatives) should satisfy (valid.DefCall)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.DefCall(target, method, targs, argss) =>
        x shouldBe alphaEqTo (core.DefCall(target, method, targs, argss))
      }
    }
  }

  "Local method definitions" - {
    // modeled by
    // - `DefDef` objects in `Core.Lang`
    // - `DefDef` nodes nodes in Scala ASTs
    val n = 2

    val examples = extractFrom(reify {
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
      (greet(), add(1, 2), timesN(3), times2N(4))
    })

    val negatives = extractFrom(reify {
      // body not a let-expr
      def mul(x: Int, y: Int) = x * y
      mul(3, 4)
    })

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.DefDef)
      no (negatives) should satisfy (valid.DefDef)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.DefDef(method, tparams, paramss, body) =>
        x shouldBe alphaEqTo (core.DefDef(method, tparams,
          paramss.map(_.map(_.symbol.asTerm)), body))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Terms
  // ---------------------------------------------------------------------------

  "Branches" - {
    // modeled by
    // - `Branch` objects in `Core.Language`
    // - `If(cond, thn, els)` nodes nodes in Scala ASTs
    val x = this.x
    val maybe = false

    val examples = extractFrom(reify {
      def add(x: Int, y: Int) = x + y
      println(if (true) "yes" else "no") // literals
      println(if (maybe) x) // references
      if (maybe) add(x, 5) else add(x, -5) // local method calls
      ()
    }).flatMap(_.collect {
      case branch: u.If => branch
    })

    val negatives = extractFrom(reify(
      if (true) x.toString else t._2, // complex branches
      if (t._1 > 42) x // complex condition
    ))

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Branch)
      no (negatives) should satisfy (valid.Branch)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.Branch(cond, thn, els) =>
        x shouldBe alphaEqTo (core.Branch(cond, thn, els))
      }
    }
  }

  "Class instantiations" - {
    // modeled by
    // - `Inst` objects in `Core.Lang`
    // - `Apply(tpt: New, args)` nodes in Scala ASTs

    implicit val converter = implicitly[CSVConverter[(Int, String, Int)]]
    val csv = CSV()

    val services = AdClass.SERVICES
    //noinspection RedundantNewCaseClass
    val examples = extractFrom(reify(
      new Ad(1, "Uber AD", services),
      new String("pi"), // inferred type-args
      new Array[Int](10), // explicit type-args
      new mutable.ListBuffer[String], // type-args only
      new Object, // no-args
      new io.csv.CSVScalaSupport[(Int, String, Int)](csv)
    ))

    val negatives = extractFrom(reify(
      new String("Hello, World!".substring(4, 8)), // method call
      new Array[Int](t._1) // selection
    ))

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Inst)
      no (negatives) should satisfy (valid.Inst)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.Inst(clazz, targs, argss) =>
        x shouldBe alphaEqTo (core.Inst(clazz, targs, argss))
      }
    }
  }

  "Lambdas" - {
    // modeled by
    // - `Lambda` objects in `Core.Lang`
    // - `Function(args, body)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify(
      (x: Int, y: Int) => { val sum = x + y; sum },
      "ellipsis".charAt _
    )).flatMap {
      case u.Block(stats, _) => stats
      case tree => Seq(tree)
    }

    val negatives = extractFrom(reify(
      // body not a let-expr
      (x: Int, y: Int) => x + y,
      (n: Int, x: Int) => {
        val n2 = n * n
        def timesN2(i: Int) = i * n2
        timesN2(x)
      }
    ))

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Lambda)
      no (negatives) should satisfy (valid.Lambda)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.Lambda(_, params, body) =>
        x shouldBe alphaEqTo (core.Lambda(params.map(_.symbol.asTerm), body))
      }
    }
  }

  "Type ascriptions" - {
    // modeled by
    // - `TypeAscr` objects in `Core.Lang`
    // - `Typed(expr, tpt)` nodes nodes in Scala ASTs
    val pi = 3.14

    val examples = extractFrom(reify(
      42: Int, // literal
      pi: Double, // reference
      "string": CharSequence, // upcast
      42: Long // coercion
    ))

    val negatives = extractFrom(reify(
      x + 42: Int, // method call
      t._2: CharSequence, // selection
      new String("3.14"): CharSequence // class instantiation
    ))

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.TypeAscr)
      no (negatives) should satisfy (valid.TypeAscr)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.TypeAscr(expr, tpe) =>
        x shouldBe alphaEqTo (core.TypeAscr(expr, tpe))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Let-in blocks
  // ---------------------------------------------------------------------------

  "Let-in blocks" - {
    // modeled by
    // - `Let` objects in `Core.Lang`
    // - `Block(stats, expr)` nodes nodes in Scala ASTs

    val examples = extractFrom(reify {
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
    })

    val negatives = extractFrom(reify {
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
    })

    examples should not be empty
    negatives should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Let)
      no (negatives) should satisfy (valid.Let)
    }

    "can be constructed and destructed" in {
      examples foreach { case x @ core.Let(vals, defs, expr) =>
        x shouldBe alphaEqTo (core.Let(vals, defs, expr))
      }
    }
  }
}
