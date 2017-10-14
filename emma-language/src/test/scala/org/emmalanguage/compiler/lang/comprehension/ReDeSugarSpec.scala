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
package compiler.lang.comprehension

import api._
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import test.schema.Marketing._

/** A spec for the `Comprehension.{re|de}sugar` transformations. */
class ReDeSugarSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf
    ).compose(_.tree)

  val resugarPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      Comprehension.resugarDataBag.timed
    ).compose(_.tree)

  val desugarPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      Comprehension.desugarDataBag.timed
    ).compose(_.tree)

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // map
  val (des1, res1) = {
    val des = reify {
      val names = users.map(u => (u.name.first, u.name.last))
      names
    }

    val res = reify {
      val users$1: test.schema.Marketing.users.type = users
      val names$1 = comprehension[(String, String), DataBag] {
        val u = generator(users$1)
        head {
          (u.name.first, u.name.last)
        }
      }
      names$1
    }

    (des, res)
  }

  // flatMap
  val (des2, res2) = {
    val des = reify {
      val names = users.flatMap(u => DataBag(Seq(u.name.first, u.name.last)))
      names
    }

    val res = reify {
      val users$1: test.schema.Marketing.users.type = users
      val names$1 = comprehension[String, DataBag] {
        val u = generator(users$1)
        val v = generator(DataBag(Seq(u.name.first, u.name.last)))
        head(v)
      }
      names$1
    }

    (des, res)
  }

  // withFilter
  val (des3, res3) = {
    val des = reify {
      val names = users.withFilter(u => u.name.first != "John")
      names
    }

    val res = reify {
      val users$1: test.schema.Marketing.users.type = users
      val names$1 = comprehension[User, DataBag] {
        val u = generator(users$1)
        guard {
          val name$1: u.name.type = u.name
          val first$1: name$1.first.type = name$1.first
          val neq$1 = first$1 != "John"
          neq$1
        }
        head {
          u
        }
      }
      names$1
    }

    (des, res)
  }

  // nested (flat)maps - 2 generators
  val (des4, res4, nor4) = {

    val des = reify {
      val users$1 = users
      val clicks$1 = clicks
      val names = users$1
        .flatMap(u => clicks$1
          .map(c => (u, c)))
      names
    }

    val res = reify {
      val users$1 = users
      val clicks$1 = clicks
      val names$1 = comprehension[(User, Click), DataBag] {
        val u = generator(users$1)
        val v = generator[(User, Click), DataBag] {
          val map$1 = comprehension[(User, Click), DataBag] {
            val c = generator(clicks$1)
            head {
              (u, c)
            }
          }
          map$1
        }
        head(v)
      }
      names$1
    }

    val nor = reify {
      val users$1 = users
      val clicks$1 = clicks
      val names = comprehension[(User, Click), DataBag] {
        val u = generator(users$1)
        val c = generator(clicks$1)
        head {
          (u, c)
        }
      }
      names
    }

    (des, res, nor)
  }

  // nested (flat)maps - 3 generators
  val (des5, res5) = {
    val des = reify {
      val names = users
        .flatMap(u => clicks
          .flatMap(c => ads
            .map(a => (a, u, c))))
      names
    }

    val res = reify {
      val users$1: test.schema.Marketing.users.type = users
      val names = comprehension[(Ad, User, Click), DataBag] {
        val u = generator(users$1)
        val v = generator[(Ad, User, Click), DataBag] {
          val clicks$1: test.schema.Marketing.clicks.type = clicks
          val flatMap$1 = comprehension[(Ad, User, Click), DataBag] {
            val c = generator(clicks$1)
            val w = generator[(Ad, User, Click), DataBag] {
              val ads$1: test.schema.Marketing.ads.type = ads
              val map$1 = comprehension[(Ad, User, Click), DataBag] {
                val a = generator(ads$1)
                head {
                  (a, u, c)
                }
              }
              map$1
            }
            head(w)
          }
          flatMap$1
        }
        head(v)
      }
      names
    }

    (des, res)
  }

  // nested (flat)maps - 3 generators and a filter
  val (des6, res6) = {
    val des = reify {
      val names = users
        .flatMap(u => clicks
          .withFilter(_.userID == u.id)
          .flatMap(c => ads.map(a => (a, u, c))))
      names
    }

    val res = reify {
      val users$1: test.schema.Marketing.users.type = users
      val names = comprehension[(Ad, User, Click), DataBag] {
        val u = generator(users$1)
        val v = generator[(Ad, User, Click), DataBag] {
          val clicks$1: test.schema.Marketing.clicks.type = clicks
          val withFilter$1 = comprehension[Click, DataBag] {
            val c = generator(clicks$1)
            guard {
              val x$1 = c.userID
              val x$2 = u.id
              val x$3 = x$1 == x$2
              x$3
            }
            head(c)
          }
          val flatMap$1 = comprehension[(Ad, User, Click), DataBag] {
            val c = generator(withFilter$1)
            val w = generator[(Ad, User, Click), DataBag] {
              val ads$1: test.schema.Marketing.ads.type = ads
              val map$1 = comprehension[(Ad, User, Click), DataBag] {
                val a = generator(ads$1)
                head {
                  (a, u, c)
                }
              }
              map$1
            }
            head(w)
          }
          flatMap$1
        }
        head(v)
      }
      names
    }

    (des, res)
  }

  // with two correlated generators
  val (des7, res7, nor7) = {
    val des = reify {
      val res = xs
        .flatMap(x => DataBag(Seq(x, x))
          .map(y => (x, y)))
      res
    }

    val res = reify {
      val xs$1: this.xs.type = xs
      val res = comprehension[(Int, Int), DataBag] {
        val x = generator[Int, DataBag] { xs$1 }
        val v = generator[(Int, Int), DataBag] {
          val xs$2 = DataBag(Seq(x, x))
          val xs$1 = comprehension[(Int, Int), DataBag] {
            val y = generator[Int, DataBag] {
              xs$2
            }
            head {
              val r = (x, y)
              r
            }
          }
          xs$1
        }
        head(v)
      }
      res
    }

    val nor = reify {
      val xs$1: this.xs.type = xs
      val res = comprehension[(Int, Int), DataBag] {
        val x = generator[Int, DataBag] { xs$1 }
        val y = generator[Int, DataBag] {
          DataBag(Seq(x, x))
        }
        head {
          (x, y)
        }
      }
      res
    }

    (des, res, nor)
  }

  // with two correlated generators and a filter
  val (des8, res8) = {
    val des = reify {
      val names = users
        .flatMap(u =>  DataBag(Seq(u.name.first, u.name.last))
          .withFilter(_.contains(u.id.toString)))
      names
    }

    val res = reify {
      val users$1: test.schema.Marketing.users.type = users
      val names = comprehension[String, DataBag] {
        val u = generator[User, DataBag] { users$1 }
        val v = generator[String, DataBag] {
          val xs$2 = DataBag(Seq(u.name.first, u.name.last))
          val xs$1 = comprehension[String, DataBag] {
            val s = generator[String, DataBag] {
              xs$2
            }
            guard { s.contains(u.id.toString) }
            head { s }
          }
          xs$1
        }
        head(v)
      }
      names
    }

    (des, res)
  }

  // degenerate flatMap
  val (des9, res9, nor9) = {
    val des = reify {
      val users$1  = users
      val clicks$1 = clicks
      val clicks$2 = users$1
        .flatMap(_ => clicks$1)
      clicks$2
    }

    val res = reify {
      val users$1  = users
      val clicks$1 = clicks
      val clicks$2 = comprehension[Click, DataBag] {
        //noinspection ScalaUnusedSymbol
        val u = generator(users$1)
        val v = generator(clicks$1)
        head(v)
      }
      clicks$2
    }

    val nor = reify {
      val users$1 = users
      val clicks$1 = clicks
      val clicks$2 = comprehension[Click, DataBag] {
        //noinspection ScalaUnusedSymbol
        val u = generator(users$1)
        val c = generator(clicks$1)
        head(c)
      }
      clicks$2
    }

    (des, res, nor)
  }
  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  "resugar" - {
    "map" in {
      resugarPipeline(des1) shouldBe alphaEqTo(anfPipeline(res1))
    }
    "flatMap" in {
      resugarPipeline(des2) shouldBe alphaEqTo(anfPipeline(res2))
    }
    "degenerate flatMap" in {
      resugarPipeline(des9) shouldBe alphaEqTo(anfPipeline(res9))
    }
    "withFilter" in {
      resugarPipeline(des3) shouldBe alphaEqTo(anfPipeline(res3))
    }
    "nested (flat)maps" - {
      "with two generators" in {
        resugarPipeline(des4) shouldBe alphaEqTo(anfPipeline(res4))
      }
      "with three generators" in {
        resugarPipeline(des5) shouldBe alphaEqTo(anfPipeline(res5))
      }
      "with three generators and one filter" in {
        resugarPipeline(des6) shouldBe alphaEqTo(anfPipeline(res6))
      }
      "with two correlated generators" in {
        resugarPipeline(des7) shouldBe alphaEqTo(anfPipeline(res7))
      }
      "with two correlated generators and a filter" in {
        resugarPipeline(des8) shouldBe alphaEqTo(anfPipeline(res8))
      }
    }
  }

  "desugar" - {

    "map" in {
      desugarPipeline(res1) shouldBe alphaEqTo(anfPipeline(des1))
    }
    "flatMap" in {
      desugarPipeline(res2) shouldBe alphaEqTo(anfPipeline(des2))
    }
    "degenerate flatMap" in {
      desugarPipeline(res9) shouldBe alphaEqTo(anfPipeline(des9))
    }
    "withFilter" in {
      desugarPipeline(res3) shouldBe alphaEqTo(anfPipeline(des3))
    }
    "nested (flat)maps" - {
      "with two generators" in {
        desugarPipeline(res4) shouldBe alphaEqTo(anfPipeline(des4))
      }
      "with three generators" in {
        desugarPipeline(res5) shouldBe alphaEqTo(anfPipeline(des5))
      }
      "with three generators and one filter" in {
        desugarPipeline(res6) shouldBe alphaEqTo(anfPipeline(des6))
      }
      "with two correlated generators" in {
        desugarPipeline(res7) shouldBe alphaEqTo(anfPipeline(des7))
      }
      "with two correlated generators and a filter" in {
        desugarPipeline(res8) shouldBe alphaEqTo(anfPipeline(des8))
      }
    }
  }

  "desugar normalized" - {
    "degenerate flatMap" in {
      desugarPipeline(nor9) shouldBe alphaEqTo(anfPipeline(des9))
    }
    "with two generators" in {
      desugarPipeline(nor4) shouldBe alphaEqTo(anfPipeline(des4))
    }
    "with two correlated generators" in {
      desugarPipeline(nor7) shouldBe alphaEqTo(anfPipeline(des7))
    }
  }
}
