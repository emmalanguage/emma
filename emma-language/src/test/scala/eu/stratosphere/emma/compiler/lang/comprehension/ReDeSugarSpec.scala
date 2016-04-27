package eu.stratosphere.emma.compiler.lang.comprehension

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import eu.stratosphere.emma.compiler.ir._
import eu.stratosphere.emma.compiler.lang.core.TreeEquality
import eu.stratosphere.emma.testschema.Marketing._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `Comprehension.{re|de}sugar` transformations. */
@RunWith(classOf[JUnitRunner])
class ReDeSugarSpec extends BaseCompilerSpec with TreeEquality {

  import compiler._
  import universe._

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /** Pipeline for this spec. */
  def typeCheckAndANF[T]: Expr[T] => Tree = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_)
  } andThen {
    Core.resolveNameClashes
  } andThen {
    Core.anf
  } andThen {
    Core.simplify
  } andThen {
    Owner.at(Owner.enclosing)
  }

  /** Normalize mock-comprehension selection chains. */
  def fix: Tree => Tree = tree => {
    val moduleSel = Tree.resolve(IR.module)
    preWalk(tree) {
      case s@Select(_, name) if IR.comprehensionOps contains s.symbol.asTerm =>
        Term.sel(moduleSel, s.symbol.asTerm)
    }
  }

  def resugar: Tree => Tree = {
    time(Comprehension.resugar(API.bagSymbol)(_), "resugar")
  } andThen {
    Owner.at(Owner.enclosing)
  }

  def desugar: Tree => Tree = {
    time(Comprehension.desugar(API.bagSymbol)(_), "desugar")
  } andThen {
    Owner.at(Owner.enclosing)
  }

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // map
  val (des1, res1) = {

    val des = typeCheckAndANF(reify {
      val names = users
        .map(u => (u.name.first, u.name.last))
      names
    })

    val res = (typeCheckAndANF andThen fix) (reify {
      val users$1 = users
      comprehension[(String, String), DataBag] {
        val u = generator(users$1)
        head {
          (u.name.first, u.name.last)
        }
      }
    })

    (des, res)
  }

  // flatMap
  val (des2, res2) = {

    val des = typeCheckAndANF(reify {
      val names = users
        .flatMap(u => DataBag(Seq(u.name.first, u.name.last)))
      names
    })

    val res = (typeCheckAndANF andThen fix) (reify {
      val users$1 = users
      flatten[String, DataBag] {
        comprehension[DataBag[String], DataBag] {
          val u = generator(users$1)
          head {
            DataBag(Seq(u.name.first, u.name.last))
          }
        }
      }
    })

    (des, res)
  }

  // withFilter
  val (des3, res3) = {

    val des = typeCheckAndANF(reify {
      val names = users
        .withFilter(u => u.name.first != "John")
      names
    })

    val res = (typeCheckAndANF andThen fix) (reify {
      val users$1 = users
      comprehension[User, DataBag] {
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
    })

    (des, res)
  }

  // nested (flat)maps - 2 generators
  val (des4, res4) = {

    val des = typeCheckAndANF(reify {
      val names = users
        .flatMap(u => clicks
          .map(c => (u, c)))
      names
    })

    val res = (typeCheckAndANF andThen fix) (reify {
      val users$1 = users
      flatten[(User, Click), DataBag] {
        comprehension[DataBag[(User, Click)], DataBag] {
          val u = generator(users$1)
          head {
            val clicks$1 = clicks
            comprehension[(User, Click), DataBag] {
              val c = generator(clicks$1)
              head {
                (u, c)
              }
            }
          }
        }
      }
    })

    (des, res)
  }

  // nested (flat)maps - 3 generators
  val (des5, res5) = {

    val des = typeCheckAndANF(reify {
      val names = users
        .flatMap(u => clicks
          .flatMap(c => ads
            .map(a => (a, u, c))))
      names
    })

    val res = (typeCheckAndANF andThen fix) (reify {
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
    })

    (des, res)
  }

  // nested (flat)maps - 3 generators and a filter
  val (des6, res6) = {

    val des = typeCheckAndANF(reify {
      val names = users
        .flatMap(u => clicks
          .withFilter(_.userID == u.id)
          .flatMap(c => ads.map(a => (a, u, c))))
      names
    })

    val res = (typeCheckAndANF andThen fix) (reify {
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
    })

    (des, res)
  }

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  "resugar" - {

    "map" in {
      resugar(des1) shouldEqual res1
    }
    "flatMap" in {
      resugar(des2) shouldEqual res2
    }
    "withFilter" in {
      resugar(des3) shouldEqual res3
    }
    "nested (flat)maps" - {
      "with two generators" in {
        resugar(des4) shouldEqual res4
      }
      "with three generators" in {
        resugar(des5) shouldEqual res5
      }
      "with three generators and one filter" in {
        resugar(des6) shouldEqual res6
      }
    }
  }

  "desugar" - {

    "map" in {
      desugar(res1) shouldEqual des1
    }
    "flatMap" in {
      desugar(res2) shouldEqual des2
    }
    "withFilter" in {
      desugar(res3) shouldEqual des3
    }
    "nested (flat)maps" - {
      "with two generators" in {
        desugar(res4) shouldEqual des4
      }
      "with three generators" in {
        desugar(res5) shouldEqual des5
      }
      "with three generators and one filter" in {
        desugar(res6) shouldEqual des6
      }
    }
  }
}
