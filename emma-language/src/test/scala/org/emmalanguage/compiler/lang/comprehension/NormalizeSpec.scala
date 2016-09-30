package org.emmalanguage
package compiler.lang.comprehension

import eu.stratosphere.emma.api.DataBag
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import testschema.Marketing._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.time.Instant

/** A spec for comprehension normalization. */
@RunWith(classOf[JUnitRunner])
class NormalizeSpec extends BaseCompilerSpec {

  import compiler._

  val lnfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      Core.inlineLetExprs
    ).compose(_.tree)

  val resugarPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      Comprehension.resugar(API.bagSymbol),
      Core.inlineLetExprs
    ).compose(_.tree)

  val normalizePipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      Comprehension.resugar(API.bagSymbol),
      Core.inlineLetExprs,
      tree => time(Comprehension.normalize(API.bagSymbol)(tree), "normalize")
    ).compose(_.tree)

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // hard coded: 2 generators
  val (inp1, exp1) = {

    val inp = u.reify {
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
    }

    val exp = u.reify {
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

    (inp, exp)
  }

  // hard coded: 3 generators
  val (inp2, exp2) = {

    val inp = u.reify {
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

    val exp = u.reify {
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
    }

    (inp, exp)
  }

  // hard-coded: 3 generators and a filter
  val (inp3, exp3) = {

    val inp = u.reify {
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

    val exp = u.reify {
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
    }

    (inp, exp)
  }

  // resugared: three generators and two filters at the end
  val (inp4, exp4) = {

    val inp = u.reify {
      for {
        u <- users
        a <- ads
        c <- clicks
        if u.id == c.userID
        if a.id == c.adID
      } yield (c.time, a.`class`)
    }

    val exp = u.reify {
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
    }

    (inp, exp)
  }

  // resugared: three generators and two interleaved filters
  val (inp5, exp5) = {

    val inp = u.reify {
      for {
        c <- clicks
        u <- users
        if u.id == c.userID
        a <- ads
        if a.id == c.adID
      } yield (c.time, a.`class`)
    }

    val exp = u.reify {
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
    }

    (inp, exp)
  }

  // resugared: one patmat generator and no filters
  val (inp6, exp6) = {

    val inp = u.reify {
      for {
        Click(adID, userID, time) <- clicks
      } yield (adID, time)
    }

    val exp = u.reify {
      val clicks$11: DataBag[Click] = clicks
      comprehension[(Long, Instant), DataBag]({
        val check$ifrefutable$1: Click = generator[Click, DataBag](clicks$11)
        head[(Long, Instant)]({
          val adID$6: Long = check$ifrefutable$1.adID
          val time$6: Instant = check$ifrefutable$1.time
          Tuple2.apply[Long, Instant](adID$6, time$6)
        })
      })
    }

    (inp, exp)
  }

  // resugared: patmat with two generators and one filter
  val (inp7, exp7) = {

    val inp = u.reify {
      for {
        Click(adID, userID, time) <- clicks
        Ad(id, name, _) <- ads
        if id == adID
      } yield (adID, time)
    }

    val exp = u.reify {
      val clicks$1 = clicks
      val ads$1 = ads
      comprehension[(Long, Instant), DataBag]({
        val click$1 = generator[Click, DataBag]({
          clicks$1
        })
        val ad$1 = generator[Ad, DataBag]({
          ads$1
        })
        guard({
          val adID$1 = click$1.adID
          val id$1 = ad$1.id
          id$1 == adID$1
        })
        head[(Long, Instant)]({
          val adID$2 = click$1.adID
          val time$2 = click$1.time
          Tuple2.apply[Long, Instant](adID$2, time$2)
        })
      })
    }

    (inp, exp)
  }

  val (inp8, exp8) = {

    val inp = u.reify {
      val xs = for {
        Click(adID, userID, time) <- clicks
        Ad(id, name, _) <- ads
        if id == adID
      } yield time.toEpochMilli
      xs.fetch()
    }

    val exp = u.reify {
      val clicks$1 = clicks
      val ads$1 = ads
      val xs = comprehension[Long, DataBag]({
        val click$1 = generator[Click, DataBag]({
          clicks$1
        })
        val ad$1 = generator[Ad, DataBag]({
          ads$1
        })
        guard({
          val adID$1 = click$1.adID
          val time$1 = click$1.time
          val id$1 = ad$1.id
          id$1 == adID$1
        })
        head[Long]({
          val time$2 = click$1.time
          time$2.toEpochMilli
        })
      })
      xs.fetch()
    }

    (inp, exp)
  }

  val (inp9, exp9) = {

    val inp = u.reify {
      val xs = for {
        Ad(id, name, _) <- ads
        token <- DataBag(name.split("\\s+"))
      } yield token
      xs.fetch()
    }

    val exp = u.reify {
      val ads$1 = ads
      val xs = comprehension[String, DataBag]({
        val ad$1 = generator[Ad, DataBag]({
          ads$1
        })
        val token = generator[String, DataBag]({
          val x$6 = ad$1.name
          val x$7 = x$6.split("\\s+")
          val x$8 = wrapRefArray[String](x$7)
          val x$9 = DataBag(x$8)
          x$9
        })
        head[String]({
          token
        })
      })
      xs.fetch()
    }

    (inp, exp)
  }

  val (inp10, exp10) = {

    val inp = u.reify {
      // pre-process
      val xs = for {
        Ad(id, name, _) <- ads
      } yield (id, name)
      // self-join
      val ys = for {
        (id1, name1) <- xs
        (id2, name2) <- xs
        if id1 == id2
      } yield (name1, name2)
      // fetch
      ys.fetch()
    }

    val exp = u.reify {
      val ads$1 = ads
      // pre-process
      val xs = comprehension[(Long, String), DataBag]({
        val check$ifrefutable$7 = generator[Ad, DataBag]({
          ads$1
        })
        head[(Long, String)]({
          val id$29 = check$ifrefutable$7.id
          val name$19 = check$ifrefutable$7.name
          (id$29, name$19)
        })
      })
      // self-join
      val ys = comprehension[(String, String), DataBag]({
        val check$ifrefutable$8 = generator[(Long, String), DataBag]({
          xs
        })
        val check$ifrefutable$9 = generator[(Long, String), DataBag]({
          xs
        })
        guard({
          val id1$1$3 = check$ifrefutable$8._1
          val id2$2 = check$ifrefutable$9._1
          id1$1$3 == id2$2
        })
        head[(String, String)]({
          val name1$1$4 = check$ifrefutable$8._2
          val name2$1 = check$ifrefutable$9._2
          (name1$1$4, name2$1)
        })
      })
      ys.fetch()
    }

    (inp, exp)
  }

  // with two correlated generators
  val (inp11, exp11) = {

    val inp = u.reify {
      for {
        x <- xs
        y <- DataBag(Seq(x, x))
      } yield (x, y)
    }

    val exp = u.reify {
      val xs$1 = xs
      comprehension[(Int, Int), DataBag] {
        val x = generator[Int, DataBag] {
          xs$1
        }
        val y = generator[Int, DataBag] {
          DataBag(Seq(x, x))
        }
        head {
          (x, y)
        }
      }
    }

    (inp, exp)
  }

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  "normalize hard-coded comprehensions" - {
    "with two generators" in {
      normalizePipeline(inp1) shouldBe alphaEqTo(lnfPipeline(exp1))
    }
    "with three generators" in {
      normalizePipeline(inp2) shouldBe alphaEqTo(lnfPipeline(exp2))
    }
    "with three generators and two filters" in {
      normalizePipeline(inp3) shouldBe alphaEqTo(lnfPipeline(exp3))
    }
  }

  "normalize resugared comprehensions" - {
    "with three generators and two filters at the end" in {
      normalizePipeline(inp4) shouldBe alphaEqTo(resugarPipeline(exp4))
    }
    "with three generators and two interleaved filters" in {
      normalizePipeline(inp5) shouldBe alphaEqTo(resugarPipeline(exp5))
    }
    "with one patmat generator and no filters" in {
      normalizePipeline(inp6) shouldBe alphaEqTo(resugarPipeline(exp6))
    }
    "with two patmat generators and one filter (at the end)" in {
      normalizePipeline(inp7) shouldBe alphaEqTo(resugarPipeline(exp7))
    }
    "with two patmat generators and one filter (itermediate result)" in {
      normalizePipeline(inp8) shouldBe alphaEqTo(resugarPipeline(exp8))
    }
    "with two correlated generators and no filters" in {
      normalizePipeline(inp9) shouldBe alphaEqTo(resugarPipeline(exp9))
    }
    "with a dag-shaped self-join" in {
      normalizePipeline(inp10) shouldBe alphaEqTo(resugarPipeline(exp10))
    }
    "with two correlated generators" in {
      normalizePipeline(inp11) shouldBe alphaEqTo(resugarPipeline(exp11))
    }
  }
}
