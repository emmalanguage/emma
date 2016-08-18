package eu.stratosphere.emma
package compiler.lang.comprehension

import api.DataBag
import compiler.BaseCompilerSpec
import compiler.ir._
import testschema.Marketing._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.time.Instant

/** A spec for comprehension normalization. */
@RunWith(classOf[JUnitRunner])
class NormalizeSpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  val anf: Expr[Any] => Tree =
    compiler.pipeline(typeCheck = true)(
      Core.resolveNameClashes,
      Core.anf,
      Core.simplify,
      normalizeLet
    ).compose(_.tree)

  val resugar: Expr[Any] => Tree =
    compiler.pipeline(typeCheck = true)(
      Core.resolveNameClashes,
      Core.anf,
      Comprehension.resugar(API.bagSymbol),
      Core.simplify
    ).compose(_.tree)

  val normalize: Expr[Any] => Tree =
    compiler.pipeline(typeCheck = true)(
      Core.resolveNameClashes,
      Core.anf,
      Core.lift,
      Core.simplify,
      Comprehension.resugar(API.bagSymbol),
      tree => time(Comprehension.normalize(API.bagSymbol)(tree), "normalize")
    ).compose(_.tree)

  // ---------------------------------------------------------------------------
  // Spec data: a collection of (desugared, resugared) trees
  // ---------------------------------------------------------------------------

  // hard coded: 2 generators
  val (inp1, exp1) = {

    val inp = reify {
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

    val exp = reify {
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

    val inp = reify {
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

    val exp = reify {
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

    val inp = reify {
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

    val exp = reify {
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

    val inp = reify {
      for {
        u <- users
        a <- ads
        c <- clicks
        if u.id == c.userID
        if a.id == c.adID
      } yield (c.time, a.`class`)
    }

    val exp = reify {
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

    val inp = reify {
      for {
        c <- clicks
        u <- users
        if u.id == c.userID
        a <- ads
        if a.id == c.adID
      } yield (c.time, a.`class`)
    }

    val exp = reify {
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

    val inp = reify {
      for {
        Click(adID, userID, time) <- clicks
      } yield (adID, time)
    }

    val exp = reify {
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
    }

    (inp, exp)
  }

  // resugared: patmat with two generators and one filter
  val (inp7, exp7) = {

    val inp = reify {
      for {
        Click(adID, userID, time) <- clicks
        Ad(id, name, _) <- ads
        if id == adID
      } yield (adID, time)
    }

    val exp = reify {
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
    }

    (inp, exp)
  }

  val (inp8, exp8) = {

    val inp = reify {
      val xs = for {
        Click(adID, userID, time) <- clicks
        Ad(id, name, _) <- ads
        if id == adID
      } yield time.toEpochMilli
      xs.fetch()
    }

    val exp = reify {
      val clicks$1 = clicks;
      val ads$1 = ads;
      val xs = comprehension[Long, DataBag]({
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
        head[Long]({
          val click$3 = click$1: Click@unchecked
          val time$2 = click$3.time;
          time$2.toEpochMilli
        })
      })
      xs.fetch()
    }

    (inp, exp)
  }

  val (inp9, exp9) = {

    val inp = reify {
      val xs = for {
        Ad(id, name, _) <- ads
        token <- DataBag(name.split("\\s+"))
      } yield token
      xs.fetch()
    }

    val exp = reify {
      val ads$1 = ads;
      val xs = comprehension[String, DataBag]({
        val ad$1 = generator[Ad, DataBag]({
          ads$1
        });
        val token = generator[String, DataBag]({
          val x$5: Ad = ad$1: Ad@unchecked;
          val x$6 = x$5.name
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

    val inp = reify {
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

    val exp = reify {
      val ads$1 = ads
      // pre-process
      val xs = comprehension[(Long, String), DataBag]({
        val check$ifrefutable$7 = generator[Ad, DataBag]({
          ads$1
        })
        head[(Long, String)]({
          val x$16 = check$ifrefutable$7: Ad
          val id$29 = x$16.id
          val name$19 = x$16.name
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
          val x$21$1$3 = check$ifrefutable$8: (Long, String)
          val id1$1$3 = x$21$1$3._1
          val x$19 = check$ifrefutable$9: (Long, String)
          val id2$2 = x$19._1
          id1$1$3 == id2$2
        })
        head[(String, String)]({
          val x$21$1$4 = check$ifrefutable$8: (Long, String)
          val name1$1$4 = x$21$1$4._2
          val x$20 = check$ifrefutable$9: (Long, String)
          val name2$1 = x$20._2
          (name1$1$4, name2$1)
        })
      })
      ys.fetch()
    }

    (inp, exp)
  }

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  "normalize hard-coded comprehensions" - {
    "with two generators" in {
      normalize(inp1) shouldBe alphaEqTo(anf(exp1))
    }
    "with three generators" in {
      normalize(inp2) shouldBe alphaEqTo(anf(exp2))
    }
    "with three generators and two filters" in {
      normalize(inp3) shouldBe alphaEqTo(anf(exp3))
    }
  }

  "normalize resugared comprehensions" - {
    "with three generators and two filters at the end" in {
      normalize(inp4) shouldBe alphaEqTo(resugar(exp4))
    }
    "with three generators and two interleaved filters" in {
      normalize(inp5) shouldBe alphaEqTo(resugar(exp5))
    }
    "with one patmat generator and no filters" in {
      normalize(inp6) shouldBe alphaEqTo(resugar(exp6))
    }
    "with two patmat generators and one filter (at the end)" in {
      normalize(inp7) shouldBe alphaEqTo(resugar(exp7))
    }
    "with two patmat generators and one filter (itermediate result)" in {
      normalize(inp8) shouldBe alphaEqTo(resugar(exp8))
    }
    "with two correlated generators and no filters" in {
      normalize(inp9) shouldBe alphaEqTo(resugar(exp9))
    }
    "with a dag-shaped self-join" in {
      normalize(inp10) shouldBe alphaEqTo(resugar(exp10))
    }
  }

  "complete examples" - {

    "Transitive closure" in {
      import eu.stratosphere.emma.api._
      import NormalizeSpec.Edge

      val input = "input"
      val output = "output"
      implicit val csv = implicitly[CSVConverters[Edge[Long]]]

      val act = normalize(u.reify {
        // read in a directed graph
        var edges = read(input, new CSVInputFormat[Edge[Long]]).distinct()
        var count = edges.size
        var added = 0l

        do {
          val closure = for {
            e1 <- edges
            e2 <- edges
            if e1.dst == e2.src
          } yield Edge(e1.src, e2.dst)
          edges = edges.plus(closure).distinct()
          val oldCount = count
          count = edges.size
          added = count - oldCount
        } while (added > 0)

        write(output, new CSVOutputFormat[Edge[Long]])(edges)
      })

      val exp = anf(u.reify {
        // read in a directed graph
        val edges$1 = read(input, new CSVInputFormat[Edge[Long]]).distinct()
        val count$1 = edges$1.size
        val added$1 = 0l
        def doWhile$1(added$3: Long, count$3: Long, edges$3: DataBag[Edge[Long]]): Unit = {
          val closure = comprehension[Edge[Long], DataBag] {
            val e1 = generator[Edge[Long], DataBag]({ edges$3 })
            val e2 = generator[Edge[Long], DataBag]({ edges$3 })
            guard {
              val dst$1 = e1.dst
              val src$1 = e2.src
              dst$1 == src$1
            }
            head {
              val src$2 = e1.src
              val dst$2 = e2.dst
              Edge(src$2, dst$2)
            }
          }
          val edges$2 = edges$3.plus(closure).distinct()
          val oldCount = count$3
          val count$2 = edges$2.size
          val added$2 = count$2 - oldCount
          val greater$1 = added$2 > 0
          def suffix$1(): Unit = {
            write(output, new CSVOutputFormat()(csv))(edges$2)
          }
          if (greater$1) doWhile$1(added$2, count$2, edges$2)
          else suffix$1()
        }
        doWhile$1(added$1, count$1, edges$1)
      })

      act shouldBe alphaEqTo (exp)
    }
  }
}

object NormalizeSpec {
  import api.model._

  case class Edge[VT](@id src: VT, @id dst: VT) extends Identity[Edge[VT]] {
    def identity = Edge(src, dst)
  }
}
