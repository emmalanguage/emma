package eu.stratosphere.emma.compiler.lang.comprehension

import java.time.Instant

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import eu.stratosphere.emma.compiler.ir._
import eu.stratosphere.emma.testschema.Marketing._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for comprehension normalization. */
@RunWith(classOf[JUnitRunner])
class NormalizeSpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  lazy val normalize: Tree => Tree = {
    time(Comprehension.normalize(API.bagSymbol)(_), "normalize")
  } andThen {
    api.Owner.at(get.enclosingOwner)
  }

  lazy val anfPipeline: Expr[Any] => Tree =
    compiler.pipeline(typeCheck = true)(
      Core.resolveNameClashes,
      Core.anf,
      Core.simplify,
      Comprehension.resugar(API.bagSymbol)
    ).compose(_.tree)

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // hard coded: 2 generators
  val (inp1, exp1) = {

    val inp = anfPipeline(reify {
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

    val exp = anfPipeline(reify {
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
    })

    (inp, exp)
  }

  // hard coded: 3 generators
  val (inp2, exp2) = {

    val inp = anfPipeline(reify {
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

    val exp = anfPipeline(reify {
      val users$1 = users
      val clicks$1 = clicks
      val ads$1 = ads
      val names = comprehension[(Ad, User, Click), DataBag] {
        val u = generator(users$1)
        val c = generator(clicks$1)
        val a = generator(ads$1)
        head {
          (a, u, c)
        }
      }
      names
    })

    (inp, exp)
  }

  // hard-coded: 3 generators and a filter
  val (inp3, exp3) = {

    val inp = anfPipeline(reify {
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

    val exp = anfPipeline(reify {
      val users$1 = users
      val clicks$1 = clicks
      val ads$1 = ads
      val names = comprehension[(Ad, User, Click), DataBag] {
        val u = generator(users$1)
        val c = generator(clicks$1)
        guard {
          val x$1 = c.userID
          val x$2 = u.id
          val x$3 = x$1 == x$2
          x$3
        }
        val a = generator(ads$1)
        head {
          (a, u, c)
        }
      }
      names
    })

    (inp, exp)
  }

  // resugared: three generators and two filters at the end
  val (inp4, exp4) = {

    val inp = anfPipeline(reify {
      for {
        u <- users
        a <- ads
        c <- clicks
        if u.id == c.userID
        if a.id == c.adID
      } yield (c.time, a.`class`)
    })

    val exp = anfPipeline(reify {
      val users$1 = users
      val ads$1 = ads
      val clicks$1 = clicks
      comprehension[(Instant, AdClass.Value), DataBag] {
        val u = generator(users$1)
        val a = generator(ads$1)
        val c = generator(clicks$1)
        guard(u.id == c.userID)
        guard(a.id == c.adID)
        head(c.time, a.`class`)
      }
    })

    (inp, exp)
  }

  // resugared: three generators and two interleaved filters
  val (inp5, exp5) = {

    val inp = anfPipeline(reify {
      for {
        c <- clicks
        u <- users
        if u.id == c.userID
        a <- ads
        if a.id == c.adID
      } yield (c.time, a.`class`)
    })

    val exp = anfPipeline(reify {
      val clicks$1 = clicks
      val users$1 = users
      val ads$1 = ads
      comprehension[(Instant, AdClass.Value), DataBag] {
        val c = generator(clicks$1)
        val u = generator(users$1)
        guard(u.id == c.userID)
        val a = generator(ads$1)
        guard(a.id == c.adID)
        head(c.time, a.`class`)
      }
    })

    (inp, exp)
  }

  // resugared: one patmat generator and no filters
  val (inp6, exp6) = {

    val inp = anfPipeline(reify {
      for {
        Click(adID, userID, time) <- clicks
      } yield (adID, time)
    })

    val exp = anfPipeline(reify {
      val clicks$11: DataBag[Click] = clicks;
      comprehension[(Long, Instant), DataBag]({
        val check$ifrefutable$1: Click = generator[Click, DataBag](clicks$11);
        head[(Long, Instant)]({
          val x$2: Click@unchecked = check$ifrefutable$1: Click@unchecked;
          val adID$6: Long = x$2.adID;
          val time$6: Instant = x$2.time;
          Tuple2.apply[Long, Instant](adID$6, time$6)
        })
      })
    })

    (inp, exp)
  }

  // resugared: patmat with two generators and one filter
  val (inp7, exp7) = {

    val inp = anfPipeline(reify {
      for {
        Click(adID, userID, time) <- clicks
        Ad(id, name, _) <- ads
        if id == adID
      } yield (adID, time)
    })

    val exp = anfPipeline(reify {
      val clicks$1 = clicks;
      val ads$1 = ads;
      comprehension[(Long, Instant), DataBag]({
        val click$1 = generator[Click, DataBag]({
          clicks$1
        });
        val ad$1 = generator[Ad, DataBag]({
          ads$1
        });
        guard({
          val click$2 = click$1: Click@unchecked
          val adID$1 = click$2.adID
          val time$1 = click$2.time
          val x$5: Ad = ad$1: Ad@unchecked;
          val ad$2 = ad$1: Ad
          val id$1 = ad$2.id
          id$1 == adID$1
        });
        head[(Long, Instant)]({
          val click$3 = click$1: Click@unchecked
          val adID$2 = click$3.adID;
          val time$2 = click$3.time;
          Tuple2.apply[Long, Instant](adID$2, time$2)
        })
      })
    })

    (inp, exp)
  }

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  "normalize hard-coded comprehensions" - {
    "with two generators" in {
      normalize(inp1) shouldBe alphaEqTo(exp1)
    }
    "with three generators" in {
      normalize(inp2) shouldBe alphaEqTo(exp2)
    }
    "with three generators and two filters" in {
      normalize(inp3) shouldBe alphaEqTo(exp3)
    }
  }

  "normalize resugared comprehensions" - {
    "with three generators and two filters at the end" in {
      normalize(inp4) shouldBe alphaEqTo(exp4)
    }
    "with three generators and two interleaved filters" in {
      normalize(inp5) shouldBe alphaEqTo(exp5)
    }
    "with one patmat generator and no filters" in {
      normalize(inp6) shouldBe alphaEqTo(exp6)
    }
    "with two patmat generators and one filter" in {
      normalize(inp7) shouldBe alphaEqTo(exp7)
    }
  }
}
