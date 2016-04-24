package eu.stratosphere.emma
package compiler.ir.lnf

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import eu.stratosphere.emma.compiler.ir._
import eu.stratosphere.emma.testschema.Marketing._
import eu.stratosphere.emma.testschema.Math._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `LNF.anf` transformation. */
@RunWith(classOf[JUnitRunner])
class ANFSpec extends BaseCompilerSpec with TreeEquality {

  import compiler._
  import universe._

  def typeCheckAndANF[T]: Expr[T] => Tree = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_)
  } andThen {
    LNF.destructPatternMatches
  } andThen {
    LNF.resolveNameClashes
  } andThen {
    time(LNF.anf(_), "anf")
  } andThen {
    Owner.at(Owner.enclosing)
  }

  "field selections" - {

    "as argument" in {
      val act = typeCheckAndANF(reify {
        15 * t._1
      })

      val exp = typeCheck(reify {
        val x$1 = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldEqual exp
    }

    "as selection" in {
      val act = typeCheckAndANF(reify {
        (t._1 * 15).toDouble
      })

      val exp = typeCheck(reify {
        val t$1 = t
        val t_1$1 = t$1._1
        val prod$1 = t_1$1 * 15
        val d$1 = prod$1.toDouble
        d$1
      })

      act shouldEqual exp
    }

    "package selections" in {
      val act = typeCheckAndANF(reify {
        val bag = eu.stratosphere.emma.api.DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.fetch())
      })

      val exp = typeCheck(reify {
        val x$1 = Seq(1, 2, 3)
        val bag = eu.stratosphere.emma.api.DataBag(x$1)
        val x$2 = bag.fetch()
        val x$3 = scala.Predef.println(x$2)
        x$3
      })

      act shouldEqual exp
    }
  }

  "complex arguments" - {
    "lhs" in {
      val act = typeCheckAndANF(reify {
        y.substring(y.indexOf('l') + 1)
      })

      val exp = typeCheckAndANF(reify {
        val y$1 = y
        val y$2 = y
        val x$1 = y$2.indexOf('l')
        val x$2 = x$1 + 1
        val x$3 = y$1.substring(x$2)
        x$3
      })

      act shouldEqual exp
    }

    "with multiple parameter lists" in {
      val act = typeCheckAndANF(reify {
        List(1, 2, 3).foldLeft(0)(_ * _)
      })

      val exp = typeCheckAndANF(reify {
        val list$1 = List(1, 2, 3)
        val mult$1 = (x: Int, y: Int) => {
          val prod$1 = x * y
          prod$1
        }
        val prod$2 = list$1.foldLeft(0)(mult$1)
        prod$2
      })

      act shouldEqual exp
    }
  }

  "nested blocks" in {
    val act = typeCheckAndANF(reify {
      val a = {
        val b = y.indexOf('T')
        b + 15
      }
      val c = {
        val b = y.indexOf('a')
        b + 15
      }
    })

    val exp = typeCheck(reify {
      val y$1 = y
      val b$1 = y$1.indexOf('T')
      val a = b$1 + 15
      val y$2 = y
      val b$2 = y$2.indexOf('a')
      val c = b$2 + 15
    })

    act shouldEqual exp
  }

  "type ascriptions" in {
    val act = typeCheckAndANF(reify {
      val a = x: Int
      val b = a + 5
      b
    })

    val exp = typeCheck(reify {
      val a$1 = x
      val a$2 = a$1: Int
      val b$1 = a$2 + 5
      b$1
    })

    act shouldEqual exp
  }

  "bypass mock comprehensions" in {

    val act = typeCheckAndANF(reify {
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

    val exp = typeCheck(reify {
      val res = comprehension {
        val u = generator[User, DataBag] {
          val u$1 = users
          u$1
        }
        val a = generator[Ad, DataBag] {
          val a$1 = ads
          a$1
        }
        val c = generator[Click, DataBag] {
          val c$1 = clicks
          c$1
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

    act shouldEqual exp
  }

  "irrefutable pattern matching" - {
    "of tuples" in {
      val act = typeCheckAndANF(reify {
        ("life", 42) match {
          case (s: String, i: Int) =>
            s + i
        }
      })

      val exp = typeCheck(reify {
        val x$1 = ("life", 42)
        val s = x$1._1
        val i = x$1._2
        val x$2 = s + i
        x$2
      })

      act shouldEqual exp
    }

    "of case classes" in {
      val act = typeCheckAndANF(reify {
        Point(1, 2) match {
          case Point(i, j) =>
            i + j
        }
      })

      val exp = typeCheck(reify {
        val p$1 = Point
        val x$1 = p$1(1, 2)
        val i = x$1.x
        val j = x$1.y
        val x$2 = i + j
        x$2
      })

      act shouldEqual exp
    }

    "of nested product types" in {
      val act = typeCheckAndANF(reify {
        (Point(1, 2), (3, 4)) match {
          case (a@Point(i, j), b@(k, _)) =>
            i + j + k
        }
      })

      val exp = typeCheck(reify {
        val p$1 = Point
        val x$1 = p$1(1, 2)
        val x$2 = (3, 4)
        val x$3 = (x$1, x$2)
        val a = x$3._1
        val i = a.x
        val j = a.y
        val b = x$3._2
        val k = b._1
        val x$4 = i + j
        val x$5 = x$4 + k
        x$5
      })

      act shouldEqual exp
    }
  }
}
