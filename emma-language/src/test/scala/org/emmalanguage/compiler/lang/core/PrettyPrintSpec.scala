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
import compiler.ir.ComprehensionSyntax._
import test.schema.Marketing._

import java.time.Instant

/** A spec for the `LNF.cse` transformation. */
class PrettyPrintSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify
  import Core.{Lang => core}

  val liftPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(Core.lnf)
      .andThen(unQualifyStatics).compose(_.tree)

  val prettyPrint: u.Tree => String =
    tree => time(Core.prettyPrint(tree), "pretty print")

  "Atomics:" - {
    "Lit" in {
      val acts = idPipeline(reify(
        42, 42L, 3.14, 3.14F, .1e6, 'c', "string"
      )) collect { case act @ core.Lit(_) =>
        prettyPrint(act)
      }

      val exps = Seq("42", "42L", "3.14", "3.14F", "100000.0", "'c'", "\"string\"")

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }

    "Ref" in {
      val acts = idPipeline(reify {
        val x = 1
        val y = 2
        val * = 3
        val `p$^s` = 4
        val ⋈ = 5
        val `foo and bar` = 6
        x * y * `*` * `p$^s` * ⋈ * `foo and bar`
      }) collect { case act @ core.Ref(_) =>
        prettyPrint(act)
      }

      val exps = Seq("x", "y", "`*`", "`p$^s`", "`⋈`", "`foo and bar`")

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }

    "This" in {
      class Unqualified {
        val expr = emma.prettyPrint {
          println(this)
        }
      }

      class Qualified {
        val expr = emma.prettyPrint {
          println(Qualified.this)
        }
      }

      class Inner {
        val expr = emma.prettyPrint {
          println(PrettyPrintSpec.this)
        }
      }

      object Module {
        val expr = emma.prettyPrint {
          println(this)
        }
      }

      val acts = Seq(
        new Unqualified().expr,
        new Qualified().expr,
        new Inner().expr,
        Module.expr)

      val exps = Seq(
        "Predef println Unqualified.this",
        "Predef println Qualified.this",
        "Predef println PrettyPrintSpec.this",
        "Predef println Module.this")

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }
  }

  "Definitions:" - {

    "DefDef" in {
      val acts = idPipeline(reify {
        def fn1(needle: Char, haystack: String): Int = {
          val z = needle.toInt
          haystack indexOf z
        }
        fn1('o', "Hello, World!")
      }) match { case u.Block(stats, _) =>
        stats map prettyPrint
      }

      val exps = Seq("""
        |def fn1(needle: Char, haystack: String): Int = {
        |  val z = needle.toInt
        |  haystack indexOf z
        |}
      """.stripMargin.trim)

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }
  }

  "Other:" - {
    "TypeAscr" in {
      val pi = 3.14

      val acts = (idPipeline(reify {
        val x = 42: Int // literal
        val y = pi: Double // reference
        val u = "string": CharSequence // upcast
        val v = 42: Long // coercion
        (x, y, u, v)
      }) collect { case u.Block(stats, _) =>
        stats collect { case u.ValDef(_, _, _, rhs) =>
          prettyPrint(rhs)
        }
      }).flatten

      val exps = """
        |42: Int
        |pi: Double
        |"string": CharSequence
        |42L: Long
      """.stripMargin.trim.split('\n')

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }

    "DefCall" in {
      val n = 42
      implicit val pair = 3.14 -> "pi"
      val c = clicks.collect().head
      val a = ads.collect().head

      val acts = (idPipeline(reify {
        def summon[A] = implicitly[(Double, String)]
        //@formatter:off
        val x$01 = Predef.println("string")                  // literal
        val x$02 = n - 2                                     // reference in target position
        val x$03 = 2 - n                                     // reference in argument position
        val x$04 = -n                                        // unary operator
        val x$05 = Seq.fill(n)('!')                          // multiple parameter lists
        val x$06 = 3.14.toString                             // 0-arg method
        val x$07 = scala.collection.Set.empty[(String, Int)] // type-args only, with target
        val x$08 = summon[(String, Int)]                     // type-args only, no target
        val x$09 = Predef.implicitly[(Double, String)]       // implicit args
        val x$10 = (c.time, a.`class`)                       // Tuple constructor, keywords
        // this.wait(5)                                      // `this` reference FIXME: does not work
        (x$01, x$02, x$03, x$04, x$05, x$06, x$07, x$08, x$09, x$10)
        //@formatter:on
      }) collect { case u.Block(stats, _) =>
        stats.tail collect { case u.ValDef(_, _, _, rhs) =>
          prettyPrint(rhs)
        }
      }).flatten

      val exps = """
        |Predef println "string"
        |n - 2
        |2 - n
        |-n
        |Seq.fill(n)('!')
        |3.14.toString()
        |Set.empty[(String, Int)]
        |summon[(String, Int)]
        |Predef implicitly pair
        |(c.time, a.`class`)
        |this wait 5L
      """.stripMargin.trim.split('\n')

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }

    "Lambda" in {
      val acts = idPipeline(reify {
        val fn1 = (needle: Char, haystack: String) => {
          val z = needle.toInt
          haystack indexOf z
        }
        val fn2 = (xs: DataBag[(Int, Int)], fn: ((Int, Int)) => Int) => {
          val ys = xs map fn
          ys
        }
        (fn1('o', "Hello, World!"), fn2(DataBag(Seq.empty), _._1))
      }) match { case u.Block(stats, _) =>
        for (u.ValDef(_, _, _, rhs) <- stats)
          yield prettyPrint(rhs)
      }

      val exps = Seq("""
        |(needle: Char, haystack: String) => {
        |  val z = needle.toInt
        |  haystack indexOf z
        |}
      """, """
        |(xs: DataBag[(Int, Int)], fn: ((Int, Int)) => Int) => {
        |  val ys = xs map fn
        |  ys
        |}
      """).map(_.stripMargin.trim)

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }

    "Inst" in {
      val services = AdClass.SERVICES

      //noinspection RedundantNewCaseClass
      val acts = idPipeline(reify {
        //@formatter:off
        //noinspection RedundantNewCaseClass
        new Ad(1L, "Uber AD", services)                 // args
        new String("pi")                                // type-args and args
        new scala.collection.mutable.ListBuffer[String] // type-args only
        new Object                                      // no-args
        ()
        //@formatter:on
      }) match { case u.Block(stats, _) =>
        stats map Pickle.prettyPrint
      }

      val exps = """
        |new Ad(1L, "Uber AD", services)
        |new String("pi")
        |new ListBuffer[String]()
        |new Object()
      """.stripMargin.trim.split("\n")

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }

    "Branch" in {
      val pi = 3.14

      val acts = idPipeline(reify {
        //@formatter:off
        def then$1(x: Int, y: Double) = 2 * x * y
        def else$1(x: Int, y: Double) = 2 * x * y
        if (pi == 3.14) then$1(1, 16.0) else else$1(3, pi)
        ()
        //@formatter:on
      }) match { case u.Block(stats, _) =>
        for (branch @ core.Branch(_, _, _) <- stats)
          yield prettyPrint(branch)
      }

      val exps = """
        |if (pi == 3.14) then$1(1, 16.0) else else$1(3, pi)
      """.stripMargin.trim.split("\n")

      (acts zip exps) foreach { case (act, exp) =>
        act shouldEqual exp
      }
    }

    "Let" in {
      val act = prettyPrint(idPipeline(reify {
        val x = 15
        val y = {
          val a = 15
          val z = 3
          a - z
        }
        def thn(x1: Int, y1: Int): Int = {
          val r1 = x1 * 7
          r1 * 42
        }
        def els(x2: Int, y2: Int): Int = {
          val r2 = x2 * 2
          r2 * 24
        }
        if (x > 0) thn(x, y) else els(x, y)
      }))

      val exp = """
        |{
        |  val x = 15
        |  val y = {
        |    val a = 15
        |    val z = 3
        |    a - z
        |  }
        |  def thn(x1: Int, y1: Int): Int = {
        |    val r1 = x1 * 7
        |    r1 * 42
        |  }
        |  def els(x2: Int, y2: Int): Int = {
        |    val r2 = x2 * 2
        |    r2 * 24
        |  }
        |  if (x > 0) thn(x, y) else els(x, y)
        |}""".stripMargin.trim

      act shouldEqual exp
    }
  }

  "Comprehensions:" - {
    "with three generators and two interleaved filters" in {
      val act = prettyPrint(liftPipeline(reify {
        val clicks$1 = clicks
        val users$1 = users
        val ads$1 = ads
        val compr$r1 = comprehension[(Instant, AdClass.Value), DataBag] {
          val c = generator(clicks$1)
          val u = generator(users$1)
          guard {
            val id$1 = u.id
            val userID = c.userID
            val eq$1 = id$1 == userID
            eq$1
          }
          val a = generator(ads$1)
          guard {
            val id$2 = a.id
            val adID = c.adID
            val eq$2 = id$2 == adID
            eq$2
          }
          head {
            val time = c.time
            val clas = a.`class`
            val tuple$1 = (time, clas)
            tuple$1
          }
        }
        compr$r1
      }))

      val exp = """
        |{
        |  val clicks$1 = Marketing.clicks
        |  val users$1 = Marketing.users
        |  val ads$1 = Marketing.ads
        |  val compr$r1 = for {
        |    c <- {
        |      clicks$1
        |    }
        |    u <- {
        |      users$1
        |    }
        |    if {
        |      val id$1 = u.id
        |      val userID = c.userID
        |      val eq$1 = id$1 == userID
        |      eq$1
        |    }
        |    a <- {
        |      ads$1
        |    }
        |    if {
        |      val id$2 = a.id
        |      val adID = c.adID
        |      val eq$2 = id$2 == adID
        |      eq$2
        |    }
        |  } yield {
        |    val time = c.time
        |    val clas = a.`class`
        |    val tuple$1 = (time, clas)
        |    tuple$1
        |  }
        |  compr$r1
        |}""".stripMargin.trim

      act shouldEqual exp
    }
  }
}
