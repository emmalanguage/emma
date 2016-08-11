package eu.stratosphere.emma
package compiler.lang.comprehension

import api.DataBag
import compiler.BaseCompilerSpec
import compiler.ir._
import testschema.Marketing._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `Comprehension.{re|de}sugar` transformations. */
@RunWith(classOf[JUnitRunner])
class ReDeSugarSpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  val anf: Expr[Any] => Tree =
    compiler.pipeline(typeCheck = true)(
      Core.resolveNameClashes,
      Core.anf
    ).compose(_.tree)

  val resugar: Expr[Any] => Tree = {
    val resugar = Comprehension.resugar(API.bagSymbol)
    compiler.pipeline(typeCheck = true)(
      Core.resolveNameClashes,
      Core.anf,
      tree => time(resugar(tree), "resugar")
    ).compose(_.tree)
  }

  val desugar: Expr[Any] => Tree = {
    val desugar = Comprehension.desugar(API.bagSymbol)
    compiler.pipeline(typeCheck = true)(
      Core.resolveNameClashes,
      Core.anf,
      tree => time(desugar(tree), "desugar")
    ).compose(_.tree)
  }

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // map
  val (des1, res1) = {

    val des = reify {
      val names = users
        .map(u => (u.name.first, u.name.last))
      names
    }

    val res = reify {
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

    val des = reify {
      val names = users
        .flatMap(u => DataBag(Seq(u.name.first, u.name.last)))
      names
    }

    val res = reify {
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

    val des = reify {
      val names = users
        .withFilter(u => u.name.first != "John")
      names
    }

    val res = reify {
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
  val (des4, res4) = {

    val des = reify {
      val names = users
        .flatMap(u => clicks
          .map(c => (u, c)))
      names
    }

    val res = reify {
      val users$1 = users
      val names$1 = flatten[(User, Click), DataBag] {
        comprehension[DataBag[(User, Click)], DataBag] {
          val u = generator(users$1)
          head {
            val clicks$1 = clicks
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

    (des, res)
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

    val des = reify {
      val names = users
        .flatMap(u => clicks
          .withFilter(_.userID == u.id)
          .flatMap(c => ads.map(a => (a, u, c))))
      names
    }

    val res = reify {
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

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  "resugar" - {

    "map" in {
      resugar(des1) shouldBe alphaEqTo(anf(res1))
    }
    "flatMap" in {
      resugar(des2) shouldBe alphaEqTo(anf(res2))
    }
    "withFilter" in {
      resugar(des3) shouldBe alphaEqTo(anf(res3))
    }
    "nested (flat)maps" - {
      "with two generators" in {
        resugar(des4) shouldBe alphaEqTo(anf(res4))
      }
      "with three generators" in {
        resugar(des5) shouldBe alphaEqTo(anf(res5))
      }
      "with three generators and one filter" in {
        resugar(des6) shouldBe alphaEqTo(anf(res6))
      }
    }
  }

  "desugar" - {

    "map" in {
      desugar(res1) shouldBe alphaEqTo(anf(des1))
    }
    "flatMap" in {
      desugar(res2) shouldBe alphaEqTo(anf(des2))
    }
    "withFilter" in {
      desugar(res3) shouldBe alphaEqTo(anf(des3))
    }
    "nested (flat)maps" - {
      "with two generators" in {
        desugar(res4) shouldBe alphaEqTo(anf(des4))
      }
      "with three generators" in {
        desugar(res5) shouldBe alphaEqTo(anf(des5))
      }
      "with three generators and one filter" in {
        desugar(res6) shouldBe alphaEqTo(anf(des6))
      }
    }
  }
}
