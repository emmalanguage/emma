package org.emmalanguage
package compiler.lang.core

import eu.stratosphere.emma.api.DataBag
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import testschema.Marketing._
import testschema.Math._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `ANF.transform` transformation. */
@RunWith(classOf[JUnitRunner])
class ANFSpec extends BaseCompilerSpec {

  import compiler._
  
  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      tree => time(ANF.transform(tree), "anf")
    ).compose(_.tree)

  "field selections" - {

    "as argument" in {
      val act = anfPipeline(u.reify {
        15 * t._1
      })

      val exp = idPipeline(u.reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "as selection" in {
      val act = anfPipeline(u.reify {
        (t._1 * 15).toDouble
      })

      val exp = idPipeline(u.reify {
        val t$1 = t
        val t_1$1 = t$1._1
        val prod$1 = t_1$1 * 15
        val d$1 = prod$1.toDouble
        d$1
      })

      act shouldBe alphaEqTo(exp)
    }

    "package selections" in {
      val act = anfPipeline(u.reify {
        val bag = eu.stratosphere.emma.api.DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.fetch())
      })

      val exp = idPipeline(u.reify {
        val x$1 = Seq(1, 2, 3)
        val bag = eu.stratosphere.emma.api.DataBag(x$1)
        val x$2 = bag.fetch()
        val x$3 = scala.Predef.println(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "complex arguments" - {
    "lhs" in {
      val act = anfPipeline(u.reify {
        y.substring(y.indexOf('l') + 1)
      })

      val exp = idPipeline(u.reify {
        val y$1 = y
        val y$2 = y
        val x$1 = y$2.indexOf('l')
        val x$2 = x$1 + 1
        val x$3 = y$1.substring(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "with multiple parameter lists" in {
      val act = anfPipeline(u.reify {
        List(1, 2, 3).foldLeft(0)(_ * _)
      })

      val exp = idPipeline(u.reify {
        val list$1 = List(1, 2, 3)
        val mult$1 = (x: Int, y: Int) => {
          val prod$1 = x * y
          prod$1
        }
        val prod$2 = list$1.foldLeft(0)(mult$1)
        prod$2
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "nested blocks" in {
    val act = anfPipeline(u.reify {
      val a = {
        val b = y.indexOf('T')
        b + 15
      }
      val c = {
        val b = y.indexOf('a')
        b + 15
      }
    })

    val exp = idPipeline(u.reify {
      val y$1 = y
      val b$1 = y$1.indexOf('T')
      val a = b$1 + 15
      val y$2 = y
      val b$2 = y$2.indexOf('a')
      val c = b$2 + 15
    })

    act shouldBe alphaEqTo(exp)
  }

  "type ascriptions" in {
    val act = anfPipeline(u.reify {
      (x: AnyVal).toString
    })

    val exp = idPipeline(u.reify {
      val a$1 = x
      val a$2 = a$1: AnyVal
      val b$1 = a$2.toString
      b$1
    })

    act shouldBe alphaEqTo(exp)
  }

  "bypass mock comprehensions" in {

    val act = anfPipeline(u.reify {
      val res = comprehension {
        val u = generator(users)
        val a = generator(ads)
        val c = generator(clicks)
        guard(u.id == c.userID)
        guard(a.id == c.adID)
        head(c.time, a.`class`)
      }
      res
    })

    val exp = idPipeline(u.reify {
      val res = comprehension {
        val u = generator[User, DataBag] {
          val users$1 = users
          users$1
        }
        val a = generator[Ad, DataBag] {
          val ads$1 = ads
          ads$1
        }
        val c = generator[Click, DataBag] {
          val clicks$1 = clicks
          clicks$1
        }
        guard {
          val x$1 = u.id
          val x$2 = c.userID
          val x$3 = x$1 == x$2
          x$3
        }
        guard {
          val x$4 = a.id
          val x$5 = c.adID
          val x$6 = x$4 == x$5
          x$6
        }
        head {
          val x$7 = c.time
          val x$8 = a.`class`
          val x$9 = (x$7, x$8)
          x$9
        }
      }
      res
    })

    act shouldBe alphaEqTo(exp)
  }

  "irrefutable pattern matching" - {
    "of tuples" in {
      val act = anfPipeline(u.reify {
        ("life", 42) match {
          case (s: String, i: Int) =>
            s + i
        }
      })

      val exp = idPipeline(u.reify {
        val x$1 = ("life", 42)
        val s = x$1._1
        val i = x$1._2
        val x$2 = s + i
        x$2
      })

      act shouldBe alphaEqTo(exp)
    }

    "of case classes" in {
      val act = anfPipeline(u.reify {
        Point(1, 2) match {
          case Point(i, j) =>
            i + j
        }
      })

      val exp = idPipeline(u.reify {
        val p$1 = Point(1, 2)
        val i = p$1.x
        val j = p$1.y
        val x$2 = i + j
        x$2
      })

      act shouldBe alphaEqTo(exp)
    }

    "of nested product types" in {
      val act = anfPipeline(u.reify {
        (Point(1, 2), (3, 4)) match {
          case (a@Point(i, j), b@(k, _)) =>
            i + j + k
        }
      })

      val exp = idPipeline(u.reify {
        val p$1 = Point(1, 2)
        val x$2 = (3, 4)
        val x$3 = (p$1, x$2)
        val a = x$3._1
        val i = a.x
        val j = a.y
        val b = x$3._2
        val k = b._1
        val x$4 = i + j
        val x$5 = x$4 + k
        x$5
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "method calls" in {
    val act = anfPipeline(u.reify(
      x.asInstanceOf[Long],
      List(1, 2, 3).foldLeft(1) { _ + _ },
      42.toChar,
      t._2.indexOf('f')
    ))

    val exp = idPipeline(u.reify {
      val x$1 = this.x
      val asInstanceOf$1 = x$1.asInstanceOf[Long]
      val List$1 = List(1, 2, 3)
      val sum$1 = (x: Int, y: Int) => {
        val sum$2 = x + y
        sum$2
      }
      val foldLeft$1 = List$1.foldLeft(1)(sum$1)
      val toChar$1 = 42.toChar
      val t$1 = this.t
      val _2$1 = t$1._2
      val indexOf$1 = _2$1.indexOf('f')
      val Tuple$1 = (asInstanceOf$1, foldLeft$1, toChar$1, indexOf$1)
      Tuple$1
    })

    act shouldBe alphaEqTo(exp)
  }

  "conditionals" - {
    "with simple branches" in {
      val act = anfPipeline(u.reify {
        if (t._1 < 42) "less" else "more"
      })

      val exp = idPipeline(u.reify {
        val t$1 = t
        val t_1$1 = t$1._1
        val less$1 = t_1$1 < 42
        val if$1 = if (less$1) "less" else "more"
        if$1
      })

      act shouldBe alphaEqTo(exp)
    }

    "with complex branches" in {
      val act = anfPipeline(u.reify {
        if (t._1 < 42) x + 10 else x - 10.0
      })

      val exp = idPipeline(u.reify {
        val t$1 = t
        val t_1$1 = t$1._1
        val less$1 = t_1$1 < 42
        val if$1 = if (less$1) {
          val x$1 = x
          val sum$1 = x$1 + 10
          val d$1 = sum$1.toDouble
          d$1
        } else {
          val x$2 = x
          val diff$1 = x$2 - 10.0
          diff$1
        }
        if$1
      })

      act shouldBe alphaEqTo(exp)
    }
  }
}
