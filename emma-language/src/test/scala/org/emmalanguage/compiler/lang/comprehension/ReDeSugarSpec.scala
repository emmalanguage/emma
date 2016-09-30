package org.emmalanguage
package compiler.lang.comprehension

import api.DataBag
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import testschema.Marketing._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `Comprehension.{re|de}sugar` transformations. */
@RunWith(classOf[JUnitRunner])
class ReDeSugarSpec extends BaseCompilerSpec {

  import compiler._

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf
    ).compose(_.tree)

  val resugarPipeline: u.Expr[Any] => u.Tree = {
    val resugar = Comprehension.resugar(API.bagSymbol)
    pipeline(typeCheck = true)(
      Core.lnf,
      tree => time(resugar(tree), "resugar")
    ).compose(_.tree)
  }

  val desugarPipeline: u.Expr[Any] => u.Tree = {
    val desugar = Comprehension.desugar(API.bagSymbol)
    pipeline(typeCheck = true)(
      Core.lnf,
      tree => time(desugar(tree), "desugar")
    ).compose(_.tree)
  }

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // map
  val (des1, res1) = {

    val des = u.reify {
      val names = users
        .map(u => (u.name.first, u.name.last))
      names
    }

    val res = u.reify {
      val users$1 = users
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

    val des = u.reify {
      val names = users
        .flatMap(u => DataBag(Seq(u.name.first, u.name.last)))
      names
    }

    val res = u.reify {
      val users$1 = users
      val names$1 = flatten[String, DataBag] {
        comprehension[DataBag[String], DataBag] {
          val u = generator(users$1)
          head {
            DataBag(Seq(u.name.first, u.name.last))
          }
        }
      }
      names$1
    }

    (des, res)
  }

  // withFilter
  val (des3, res3) = {

    val des = u.reify {
      val names = users
        .withFilter(u => u.name.first != "John")
      names
    }

    val res = u.reify {
      val users$1 = users
      val names$1 = comprehension[User, DataBag] {
        val u = generator(users$1)
        guard {
          val name$1 = u.name
          val first$1 = name$1.first
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

    val des = u.reify {
      val users$1 = users
      val clicks$1 = clicks
      val names = users$1
        .flatMap(u => clicks$1
          .map(c => (u, c)))
      names
    }

    val res = u.reify {
      val users$1 = users
      val clicks$1 = clicks
      val names$1 = flatten[(User, Click), DataBag] {
        comprehension[DataBag[(User, Click)], DataBag] {
          val u = generator(users$1)
          head {
            val map$1 = comprehension[(User, Click), DataBag] {
              val c = generator(clicks$1)
              head {
                (u, c)
              }
            }
            map$1
          }
        }
      }
      names$1
    }

    val nor = u.reify {
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

    val des = u.reify {
      val names = users
        .flatMap(u => clicks
          .flatMap(c => ads
            .map(a => (a, u, c))))
      names
    }

    val res = u.reify {
      val users$1 = users
      val names = flatten[(Ad, User, Click), DataBag] {
        comprehension[DataBag[(Ad, User, Click)], DataBag] {
          val u = generator(users$1)
          head {
            val clicks$1 = clicks
            val flatMap$1 = flatten[(Ad, User, Click), DataBag] {
              comprehension[DataBag[(Ad, User, Click)], DataBag] {
                val c = generator(clicks$1)
                head {
                  val ads$1 = ads
                  val map$1 = comprehension[(Ad, User, Click), DataBag] {
                    val a = generator(ads$1)
                    head {
                      (a, u, c)
                    }
                  }
                  map$1
                }
              }
            }
            flatMap$1
          }
        }
      }
      names
    }

    (des, res)
  }

  // nested (flat)maps - 3 generators and a filter
  val (des6, res6) = {

    val des = u.reify {
      val names = users
        .flatMap(u => clicks
          .withFilter(_.userID == u.id)
          .flatMap(c => ads.map(a => (a, u, c))))
      names
    }

    val res = u.reify {
      val users$1 = users
      val names = flatten[(Ad, User, Click), DataBag] {
        comprehension[DataBag[(Ad, User, Click)], DataBag] {
          val u = generator(users$1)
          head {
            val clicks$1 = clicks
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
            val flatMap$1 = flatten[(Ad, User, Click), DataBag] {
              comprehension[DataBag[(Ad, User, Click)], DataBag] {
                val c = generator(withFilter$1)
                head {
                  val ads$1 = ads
                  val map$1 = comprehension[(Ad, User, Click), DataBag] {
                    val a = generator(ads$1)
                    head {
                      (a, u, c)
                    }
                  }
                  map$1
                }
              }
            }
            flatMap$1
          }
        }
      }
      names
    }

    (des, res)
  }

  // with two correlated generators
  val (des7, res7, nor7) = {

    val des = u.reify {
      val res = xs
        .flatMap(x =>  DataBag(Seq(x, x))
          .map(y => (x, y)))
      res
    }

    val res = u.reify {
      val xs$1 = xs
      val res = flatten[(Int, Int), DataBag] {
        comprehension[DataBag[(Int, Int)], DataBag] {
          val x = generator[Int, DataBag] { xs$1 }
          head {
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
        }
      }
      res
    }

    val nor = u.reify {
      val xs$1 = xs
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

    val des = u.reify {
      val names = users
        .flatMap(u =>  DataBag(Seq(u.name.first, u.name.last))
          .withFilter(_.contains(u.id.toString)))
      names
    }

    val res = u.reify {
      val users$1 = users
      val names = flatten[String, DataBag] {
        comprehension[DataBag[String], DataBag] {
          val u = generator[User, DataBag] { users$1 }
          head {
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
        }
      }
      names
    }

    (des, res)
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
    "with two generators" in {
      desugarPipeline(nor4) shouldBe alphaEqTo(anfPipeline(des4))
    }
    "with two correlated generators" in {
      desugarPipeline(nor7) shouldBe alphaEqTo(anfPipeline(des7))
    }
  }
}
