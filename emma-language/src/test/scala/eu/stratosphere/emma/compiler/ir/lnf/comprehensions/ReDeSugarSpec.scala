package eu.stratosphere.emma.compiler.ir.lnf.comprehensions

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import eu.stratosphere.emma.compiler.ir._
import eu.stratosphere.emma.compiler.ir.lnf.TreeEquality
import eu.stratosphere.emma.testschema.Marketing._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for resugaring comprehensions. */
@RunWith(classOf[JUnitRunner])
class ReDeSugarSpec extends BaseCompilerSpec with TreeEquality {

  import compiler.IR.comprehensionOps
  import compiler.universe._

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  def typeCheckAndANF[T](expr: Expr[T]): Tree = {
    val pipeline = {
      compiler.typeCheck(_: Tree)
    } andThen {
      compiler.LNF.resolveNameClashes
    } andThen {
      compiler.LNF.anf
    }

    pipeline(expr.tree)
  }

  /**
   * Fix some issues with the expected trees.
   *
   * - Remove empty blocks.
   * - Normalize mock-comprehension selection chains.
   */
  private def fix(tree: Tree): Tree = {
    val moduleSel = compiler.Tree.resolve(compiler.IR.module)
    def transform = compiler.preWalk(tree) {
      case s@Select(_, name) if s.symbol.isTerm && (comprehensionOps contains s.symbol.asTerm) =>
        compiler.Term.sel(moduleSel, s.symbol.asTerm)
      case Block(Nil, expr) =>
        expr
      case block =>
        block
    }
    time(transform, "fix expected tree")
  }

  private def resugar(tree: Tree): Tree = {
    time(compiler.Comprehension.resugar(compiler.API.bagSymbol)(tree), "resugar")
  }

  private def desugar(tree: Tree): Tree = {
    time(compiler.Comprehension.desugar(compiler.API.bagSymbol)(tree), "desugar")
  }

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // map
  val (des1, res1) = {

    val des = typeCheckAndANF(reify {
      val names = users.map(u => (u.name.first, u.name.last))
      names
    })

    val res = (typeCheckAndANF _ andThen fix _) (reify {
      val users$1 = users
      val names = comprehension[(String, String), DataBag] {
        val u = generator(users$1)
        head {
          (u.name.first, u.name.last)
        }
      }
      names
    })

    (des, res)
  }

  // flatMap
  val (des2, res2) = {

    val des = typeCheckAndANF(reify {
      val names = users.flatMap(u => DataBag(Seq(u.name.first, u.name.last)))
      names
    })

    val res = (typeCheckAndANF _ andThen fix _) (reify {
      val users$1 = users
      val names = flatten[String, DataBag] {
        comprehension[DataBag[String], DataBag] {
          val u = generator(users$1)
          head {
            DataBag(Seq(u.name.first, u.name.last))
          }
        }
      }
      names
    })

    (des, res)
  }

  // withFilter
  val (des3, res3) = {

    val des = typeCheckAndANF(reify {
      val names = users.withFilter(u => u.name.first != "John")
      names
    })

    val res = (typeCheckAndANF _ andThen fix _) (reify {
      val users$1 = users
      val names = comprehension[User, DataBag] {
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
      names
    })

    (des, res)
  }

  // nested (flat)maps - 2 generators
  val (des4, res4) = {

    val des = typeCheckAndANF(reify {
      val names = users.flatMap(u => clicks.map(c => (u, c)))
      names
    })

    val res = (typeCheckAndANF _ andThen fix _) (reify {
      val users$1 = users
      val names = flatten[(User, Click), DataBag] {
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
      names
    })

    (des, res)
  }

  // nested (flat)maps - 3 generators
  val (des5, res5) = {

    val des = typeCheckAndANF(reify {
      val names = users.flatMap(u => clicks.flatMap(c => ads.map(a => (a, u, c))))
      names
    })

    val res = (typeCheckAndANF _ andThen fix _) (reify {
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
        desugar(des5) shouldEqual des5
      }
    }
  }
}
