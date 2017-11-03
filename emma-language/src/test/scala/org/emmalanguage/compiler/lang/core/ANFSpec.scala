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
import test.schema.Math._

/** A spec for the `ANF.transform` transformation. */
class ANFSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      ANF.transform.timed
    ).compose(_.tree)

  val A = new A
  class A {
    val B = new B
    class B {
      val C = new C
      class C {
        val x = "42"

        case object D extends Enumeration {
          val C = Value("club")
          val D = Value("diamond")
          val H = Value("heart")
          val S = Value("spade")
        }
      }
    }
  }

  def abcx(x: A.B.C.x.type) =
    x.trim.toInt

  def abcd(x: A.B.C.D.Value) =
    x.toString

  val X = ("club", "heart")

  "field selections" - {
    "as argument" in {
      val act = anfPipeline(reify {
        15 * t._1
      })

      val exp = idPipeline(reify {
        val x$1: this.t.type = t
        val x$2 = x$1._1
        val x$3 = 15 * x$2
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "as selection" in {
      val act = anfPipeline(reify {
        (t._1 * 15).toDouble
      })

      val exp = idPipeline(reify {
        val t$1: this.t.type = t
        val t_1$1 = t$1._1
        val prod$1 = t_1$1 * 15
        val d$1 = prod$1.toDouble
        d$1
      })

      act shouldBe alphaEqTo(exp)
    }

    "package selections" in {
      val act = anfPipeline(reify {
        val bag = DataBag(Seq(1, 2, 3))
        scala.Predef.println(bag.collect())
      })

      val exp = idPipeline(reify {
        val x$1 = Seq(1, 2, 3)
        val bag = DataBag(x$1)
        val x$2 = bag.collect()
        val x$3 = scala.Predef.println(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "complex arguments" - {
    "lhs" in {
      val act = anfPipeline(reify {
        y.substring(y.indexOf('l') + 1)
      })

      val exp = idPipeline(reify {
        val y$1: this.y.type = y
        val y$2: this.y.type = y
        val x$1 = y$2.indexOf('l')
        val x$2 = x$1 + 1
        val x$3 = y$1.substring(x$2)
        x$3
      })

      act shouldBe alphaEqTo(exp)
    }

    "with multiple parameter lists" in {
      val act = anfPipeline(reify {
        List(1, 2, 3).foldLeft(0)(_ * _)
      })

      val exp = idPipeline(reify {
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
    val act = anfPipeline(reify {
      val a = {
        val b = y.indexOf('T')
        b + 15
      }
      val c = {
        val b = y.indexOf('a')
        b + 15
      }
      a * c
    })

    val exp = idPipeline(reify {
      val y$1: this.y.type = y
      val b$1 = y$1.indexOf('T')
      val a = b$1 + 15
      val y$2: this.y.type = y
      val b$2 = y$2.indexOf('a')
      val c = b$2 + 15
      val prod$1 = a * c
      prod$1
    })

    act shouldBe alphaEqTo(exp)
  }

  "type ascriptions" in {
    val act = anfPipeline(reify {
      (x: AnyVal).toString
    })

    val exp = idPipeline(reify {
      val a$1 = x
      val a$2 = a$1: AnyVal
      val b$1 = a$2.toString
      b$1
    })

    act shouldBe alphaEqTo(exp)
  }

  "bypass mock comprehensions" in {
    val act = anfPipeline(reify {
      comprehension {
        val u = generator(users)
        val a = generator(ads)
        val c = generator(clicks)
        guard(u.id == c.userID)
        guard(a.id == c.adID)
        head(c.time, a.`class`)
      }
    })

    val exp = idPipeline(reify {
      val res = comprehension {
        val u = generator[User, DataBag] {
          val users$1: test.schema.Marketing.users.type = users
          users$1
        }
        val a = generator[Ad, DataBag] {
          val ads$1: test.schema.Marketing.ads.type = ads
          ads$1
        }
        val c = generator[Click, DataBag] {
          val clicks$1: test.schema.Marketing.clicks.type = clicks
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
          val x$7: c.time.type = c.time
          val x$8: a.`class`.type = a.`class`
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
      val act = anfPipeline(reify {
        ("life", 42) match {
          case (s: String, i: Int) =>
            s + i
        }
      })

      val exp = idPipeline(reify {
        val x$1 = ("life", 42)
        val s = x$1._1
        val i = x$1._2
        val x$2 = s + i
        x$2
      })

      act shouldBe alphaEqTo(exp)
    }

    "of case classes" in {
      val act = anfPipeline(reify {
        Point(1, 2) match {
          case Point(i, j) =>
            i + j
        }
      })

      val exp = idPipeline(reify {
        val p$1 = Point(1, 2)
        val i = p$1.x
        val j = p$1.y
        val x$2 = i + j
        x$2
      })

      act shouldBe alphaEqTo(exp)
    }

    "of nested product types" in {
      val act = anfPipeline(reify {
        (Point(1, 2), (3, 4)) match {
          case (a@Point(i, j), (m, n)) =>
            a.copy(x = i * m, y = j * n)
        }
      })

      val exp = idPipeline(reify {
        val point$1 = Point(1, 2)
        val pair$1 = (3, 4)
        val pair$2 = (point$1, pair$1)
        val a = pair$2._1
        val i = a.x
        val j = a.y
        val pair$3: pair$2._2.type = pair$2._2
        val m = pair$3._1
        val pair$4: pair$2._2.type = pair$2._2
        val n = pair$4._2
        val prod$1 = i * m
        val prod$2 = j * n
        val point$2 = a.copy(x = prod$1, y = prod$2)
        point$2
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "method calls" in {
    val act = anfPipeline(reify(
      x.asInstanceOf[Long],
      List(1, 2, 3).foldLeft(1) { _ + _ },
      42.toChar,
      t._2.indexOf('f')
    ))

    val exp = idPipeline(reify {
      val x$1 = this.x
      val asInstanceOf$1 = x$1.asInstanceOf[Long]
      val List$1 = List(1, 2, 3)
      val sum$1 = (x: Int, y: Int) => {
        val sum$2 = x + y
        sum$2
      }
      val foldLeft$1 = List$1.foldLeft(1)(sum$1)
      val toChar$1 = 42.toChar
      val t$1: this.t.type = this.t
      val _2$1: t$1._2.type = t$1._2
      val indexOf$1 = _2$1.indexOf('f')
      val Tuple$1 = (asInstanceOf$1, foldLeft$1, toChar$1, indexOf$1)
      Tuple$1
    })

    act shouldBe alphaEqTo(exp)
  }

  "conditionals" - {
    "with simple branches" in {
      val act = anfPipeline(reify {
        if (t._1 < 42) "less" else "more"
      })

      val exp = idPipeline(reify {
        val t$1: this.t.type = t
        val t_1$1 = t$1._1
        val less$1 = t_1$1 < 42
        val if$1 = if (less$1) "less" else "more"
        if$1
      })

      act shouldBe alphaEqTo(exp)
    }

    "with complex branches" in {
      val act = anfPipeline(reify {
        if (t._1 < 42) x + 10 else x - 10.0
      })

      val exp = idPipeline(reify {
        val t$1: this.t.type = t
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

  "variable length arguments" in {
    val act = anfPipeline(reify {
      Seq(Seq(1, 2, 3 + x): _*)
    })

    val exp = idPipeline(reify {
      val x$1 = this.x
      val sum$1 = 3 + x$1
      val Seq$1 = Seq(1, 2, sum$1)
      val Seq$2 = Seq(Seq$1: _*)
      Seq$2
    })

    act shouldBe alphaEqTo(exp)
  }

  "path-dependent types" in {
    val act = anfPipeline(reify {
      abcx(A.B.C.x)
    })

    val exp = idPipeline(reify {
      val A$1: this.A.type = this.A
      val B$1: A$1.B.type = A$1.B
      val C$1: B$1.C.type = B$1.C
      val x$1: C$1.x.type = C$1.x
      val abcx$1 = abcx(x$1)
      abcx$1
    })

    act shouldBe alphaEqTo(exp)
  }

  "enum types" - {
    "static" in {
      val act = anfPipeline(reify {
        val p1 = X._1 == ANFSpec.Enum.C.toString
        val p2 = X._1 == ANFSpec.Enum.S.toString
        val p3 = p1 && p2
        p3
      })

      val exp = idPipeline(reify {
        val X$r1: this.X.type = this.X;
        val _1$r1: X$r1._1.type = X$r1._1;
        val C$r1: ANFSpec.Enum.C.type = ANFSpec.Enum.C;
        val toString$r1 = C$r1.toString;
        val p1 = _1$r1.==(toString$r1);
        val X$r2: this.X.type = this.X;
        val _1$r2: X$r2._1.type = X$r2._1;
        val S$r1: ANFSpec.Enum.S.type = ANFSpec.Enum.S;
        val toString$r2 = S$r1.toString;
        val p2 = _1$r2.==(toString$r2);
        val p3 = p1.&&(p2);
        p3
      })

      act shouldBe alphaEqTo(exp)
    }

    "path-dependent" in {
      val act = anfPipeline(reify {
        abcd(A.B.C.D.C)
      })

      val exp = idPipeline(reify {
        val A$r1: this.A.type = this.A;
        val B$r1: A$r1.B.type = A$r1.B;
        val C$r1: B$r1.C.type = B$r1.C;
        val D$r1 = C$r1.D;
        val C$r2: D$r1.C.type = D$r1.C;
        val abcd$r1 = abcd(C$r2);
        abcd$r1
      })

      act shouldBe alphaEqTo(exp)
    }
  }
}

object ANFSpec {

  case object Enum extends Enumeration {
    val C = Value("club")
    val D = Value("diamond")
    val H = Value("heart")
    val S = Value("spade")
  }

}
