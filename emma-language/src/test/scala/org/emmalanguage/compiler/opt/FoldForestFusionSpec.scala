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
package compiler.opt

import api._
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._

import scala.util.Random

/** A spec for the fold-fusion optimization. */
class FoldForestFusionSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  def testPipeline(prefix: TreeTransform): u.Tree => u.Tree =
    pipeline(typeCheck = true)(
      prefix,
      FoldForestFusion.foldForestFusion.timed
    )

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(Core.anf).compose(_.tree)

  //@formatter:off
  val rand               = new Random()
  val rands              = DataBag(Seq.fill(100)(rand.nextInt()))
  val edges              = rands.map(x => (x, x + 1))

  val `square`           = (x: Int) => x * x
  val `is prime`         = (_: Int) => rand.nextBoolean()
  val `_ > 0`            = (x: Int) => x > 0
  val `_ < 0`            = (x: Int) => x < 0
  val `_ != 0`           = (x: Int) => x != 0
  val `+/- 5`            = (x: Int) => DataBag(x - 5 to x + 5)
  val `x => x * x`       = (x: Int) => x * x
  val `_ + 1`            = (x: Int) => x + 1
  val `_ % 2 == 0`       = (x: Int) => x % 2 == 0
  val `_ * _ % 5`        = (x: Int, y: Int) => x * y % 5
  val `_ > 0.5`          = (x: Double) => x > 0.5
  val `_ <= 0.5`         = (x: Double) => x <= 0.5
  val `_.abs min _.abs`  = (x: Int, y: Int) => x.abs min y.abs
  val `_ < _`            = (x: Int, y: Int) => x < y
  val `_ == _`           = (x: Int, y: Int) => x == y
  //@formatter:on

  "inline trivial trees" - {
    "size" in {
      val act = testPipeline(Core.anf)(reify {
        val rands = this.rands
        val reslt = rands.size
        reslt
      }.tree)

      val exp = anfPipeline(reify {
        val rands = this.rands
        val reslt = rands.fold(alg.Size)
        reslt
      })

      act shouldBe alphaEqTo(exp)
    }

    "min" in {
      val act = testPipeline(Core.anf)(reify {
        val rands = this.rands
        val reslt = rands.min
        reslt
      }.tree)

      val exp = anfPipeline(reify {
        val algb1 = alg.Min(Ordering.Int)
        val rands = this.rands
        val fold1 = rands.fold(algb1)
        val reslt = fold1.get
        reslt
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "banana-fusion" - {
    "of all algebra types" in {
      val act = testPipeline(Core.anf)(reify {
        val rands = this.rands
        (
          rands.isEmpty,
          rands.nonEmpty,
          rands.size,
          rands.min,
          rands.max,
          rands.sum,
          rands.product,
          rands.count(`_ % 2 == 0`),
          rands.exists(`_ < 0`),
          rands.forall(`_ != 0`),
          rands.find(`_ > 0`),
          rands.bottom(10),
          rands.top(10),
          rands.reduce(0)(`_ * _ % 5`),
          rands.reduceOption(`_.abs min _.abs`)
        )
      }.tree)

      val exp = anfPipeline(reify {
        val alg$Bottom$r1 = alg.Bottom(10, Ordering.Int)
        val alg$Max$r1 = alg.Max(Ordering.Int)
        val alg$Min$r1 = alg.Min(Ordering.Int)
        val alg$Product$r1 = alg.Product(Numeric.IntIsIntegral)
        val alg$Sum$r1 = alg.Sum(Numeric.IntIsIntegral)
        val alg$Top$r1 = alg.Top(10, Ordering.Int)
        val anf$r1: this.`_ % 2 == 0`.type = this.`_ % 2 == 0`
        val anf$r2: this.`_ < 0`.type = this.`_ < 0`
        val anf$r3: this.`_ != 0`.type = this.`_ != 0`
        val anf$r4: this.`_ > 0`.type = this.`_ > 0`
        val anf$r5: this.`_ * _ % 5`.type = this.`_ * _ % 5`
        val anf$r6: this.`_.abs min _.abs`.type = this.`_.abs min _.abs`
        val rands = this.rands
        val alg$Count$r1 = alg.Count(anf$r1)
        val alg$Exists$r1 = alg.Exists(anf$r2)
        val alg$Find$r1 = alg.Find(anf$r4)
        val alg$Forall$r1 = alg.Forall(anf$r3)
        val alg$Reduce$r1 = alg.Reduce(0, anf$r5)
        val alg$ReduceOpt$r1 = alg.ReduceOpt(anf$r6)
        val alg$Alg15$r1 = alg.Alg15(
          alg.IsEmpty, alg.NonEmpty, alg.Size, alg$Sum$r1, alg$Product$r1, alg$Count$r1, alg$Exists$r1, alg$Forall$r1,
          alg$Find$r1, alg$Bottom$r1, alg$Top$r1, alg$Reduce$r1, alg$ReduceOpt$r1, alg$Max$r1, alg$Min$r1)
        val app$Alg15$r1 = rands fold alg$Alg15$r1
        val anf$r45 = app$Alg15$r1._1;
        val anf$r46 = app$Alg15$r1._2;
        val anf$r47 = app$Alg15$r1._3;
        val anf$r50 = app$Alg15$r1._4;
        val anf$r51 = app$Alg15$r1._5;
        val anf$r53 = app$Alg15$r1._6;
        val anf$r55 = app$Alg15$r1._7;
        val anf$r57 = app$Alg15$r1._8;
        val anf$r59 = app$Alg15$r1._9;
        val anf$r60 = app$Alg15$r1._10;
        val anf$r61 = app$Alg15$r1._11;
        val anf$r63 = app$Alg15$r1._12;
        val anf$r65 = app$Alg15$r1._13;
        val app$Max$r1 = app$Alg15$r1._14;
        val app$Min$r1 = app$Alg15$r1._15;
        val anf$r48 = app$Min$r1.get;
        val anf$r49 = app$Max$r1.get;
        val anf$r66 = (
          anf$r45, anf$r46, anf$r47, anf$r48, anf$r49, anf$r50, anf$r51, anf$r53,
          anf$r55, anf$r57, anf$r59, anf$r60, anf$r61, anf$r63, anf$r65)
        anf$r66
      })

      act shouldBe alphaEqTo(exp)
    }

    "of more than 22 independent folds" in {
      import u._
      // More than 22 folds can still be fused via nested tuples.
      val act = testPipeline(Core.anf)(q"""
        val id = (x: Int) => x
        val sum  = (x: Int, y: Int) => scala.math.Numeric.IntIsIntegral.plus(x, y)
        val nums = org.emmalanguage.api.DataBag(1 to 100)
        ..${for (i <- 1 to 25) yield q"val ${u.TermName(f"res$i%02d")} = nums.fold($i)(id, sum)"}
        Seq(..${for (i <- 1 to 25) yield u.Ident(f"res$i%02d")})
      """)

      val exp = anfPipeline(reify {
        val anf$r1 = Predef intWrapper 1
        val id = (x: Int) => {
          x
        }
        val sum = (x$r1: Int, y: Int) => {
          val plus$r1 = Numeric.IntIsIntegral.plus(x$r1, y)
          plus$r1
        }
        val alg$fold$r1 = alg.Fold(1, id, sum)
        val alg$fold$r2 = alg.Fold(2, id, sum)
        val alg$fold$r3 = alg.Fold(3, id, sum)
        val alg$fold$r4 = alg.Fold(4, id, sum)
        val alg$fold$r5 = alg.Fold(5, id, sum)
        val alg$fold$r6 = alg.Fold(6, id, sum)
        val alg$fold$r7 = alg.Fold(7, id, sum)
        val alg$fold$r8 = alg.Fold(8, id, sum)
        val alg$fold$r9 = alg.Fold(9, id, sum)
        val alg$fold$r10 = alg.Fold(10, id, sum)
        val alg$fold$r11 = alg.Fold(11, id, sum)
        val alg$fold$r12 = alg.Fold(12, id, sum)
        val alg$fold$r13 = alg.Fold(13, id, sum)
        val alg$fold$r14 = alg.Fold(14, id, sum)
        val alg$fold$r15 = alg.Fold(15, id, sum)
        val alg$fold$r16 = alg.Fold(16, id, sum)
        val alg$fold$r17 = alg.Fold(17, id, sum)
        val alg$fold$r18 = alg.Fold(18, id, sum)
        val alg$fold$r19 = alg.Fold(19, id, sum)
        val alg$fold$r20 = alg.Fold(20, id, sum)
        val alg$fold$r21 = alg.Fold(21, id, sum)
        val alg$fold$r22 = alg.Fold(22, id, sum)
        val alg$fold$r23 = alg.Fold(23, id, sum)
        val alg$fold$r24 = alg.Fold(24, id, sum)
        val alg$fold$r25 = alg.Fold(25, id, sum)
        val to$r1 = anf$r1 to 100
        val alg$Alg22$r1 = alg.Alg22(
          alg$fold$r1, alg$fold$r2, alg$fold$r3, alg$fold$r4, alg$fold$r5, alg$fold$r6, alg$fold$r7, alg$fold$r8,
          alg$fold$r9, alg$fold$r10, alg$fold$r11, alg$fold$r12, alg$fold$r13, alg$fold$r14, alg$fold$r15,
          alg$fold$r16, alg$fold$r17, alg$fold$r18, alg$fold$r19, alg$fold$r20, alg$fold$r21, alg$fold$r22)
        val nums = DataBag(to$r1)
        val alg$Alg4$r1 = alg.Alg4(alg$Alg22$r1, alg$fold$r23, alg$fold$r24, alg$fold$r25)
        val app$Alg4$r1 = nums fold alg$Alg4$r1
        val app$Alg22$r1 = app$Alg4$r1._1
        val res23 = app$Alg4$r1._2
        val res24 = app$Alg4$r1._3
        val res25 = app$Alg4$r1._4
        val res01 = app$Alg22$r1._1
        val res02 = app$Alg22$r1._2
        val res03 = app$Alg22$r1._3
        val res04 = app$Alg22$r1._4
        val res05 = app$Alg22$r1._5
        val res06 = app$Alg22$r1._6
        val res07 = app$Alg22$r1._7
        val res08 = app$Alg22$r1._8
        val res09 = app$Alg22$r1._9
        val res10 = app$Alg22$r1._10
        val res11 = app$Alg22$r1._11
        val res12 = app$Alg22$r1._12
        val res13 = app$Alg22$r1._13
        val res14 = app$Alg22$r1._14
        val res15 = app$Alg22$r1._15
        val res16 = app$Alg22$r1._16
        val res17 = app$Alg22$r1._17
        val res18 = app$Alg22$r1._18
        val res19 = app$Alg22$r1._19
        val res20 = app$Alg22$r1._20
        val res21 = app$Alg22$r1._21
        val res22 = app$Alg22$r1._22
        val apply$r3 = Seq(
          res01, res02, res03, res04, res05, res06, res07, res08, res09, res10, res11, res12, res13, res14, res15,
          res16, res17, res18, res19, res20, res21, res22, res23, res24, res25)
        apply$r3
      })

      act shouldBe alphaEqTo(exp)
    }

    "with preceding non-linear comprehension" in {
      val act = testPipeline(Core.lift)(reify {
        val triangles = for {
          (x, u) <- edges
          (y, v) <- edges
          (z, w) <- edges
          if `_ < _`(x, u)
          if `_ < _`(y, v)
          if `_ < _`(z, w)
          if `_ == _`(u, y)
          if `_ == _`(x, z)
          if `_ == _`(v, w)
        } yield (x, u, v)

        val r1 = triangles.isEmpty
        val r2 = triangles.size

        (r1, r2)
      }.tree)

      val exp = anfPipeline(reify {
        val alg$Alg2$r1 = alg.Alg2[(Int, Int, Int), Boolean, Long](alg.IsEmpty, alg.Size);
        val edges$r1: this.edges.type = this.edges;
        val edges$r2: this.edges.type = this.edges;
        val edges$r3: this.edges.type = this.edges;
        val triangles = comprehension[(Int, Int, Int), DataBag]({
          val check$ifrefutable$1 = generator[(Int, Int), DataBag]({
            edges$r1
          });
          val check$ifrefutable$2 = generator[(Int, Int), DataBag]({
            edges$r2
          });
          val check$ifrefutable$3 = generator[(Int, Int), DataBag]({
            edges$r3
          });
          guard({
            val x$r1 = check$ifrefutable$1._1;
            val u = check$ifrefutable$1._2;
            val `_ < _$r1` : this.`_ < _`.type = this.`_ < _`;
            val apply$r18 = `_ < _$r1`.apply(x$r1, u);
            apply$r18
          });
          guard({
            val y = check$ifrefutable$2._1;
            val v$r1 = check$ifrefutable$2._2;
            val `_ < _$r2` : this.`_ < _`.type = this.`_ < _`;
            val apply$r19 = `_ < _$r2`.apply(y, v$r1);
            apply$r19
          });
          guard({
            val z = check$ifrefutable$3._1;
            val w$r1 = check$ifrefutable$3._2;
            val `_ < _$r3` : this.`_ < _`.type = this.`_ < _`;
            val apply$r20 = `_ < _$r3`.apply(z, w$r1);
            apply$r20
          });
          guard({
            val u = check$ifrefutable$1._2;
            val y = check$ifrefutable$2._1;
            val `_ == _$r1` : this.`_ == _`.type = this.`_ == _`;
            val apply$r21 = `_ == _$r1`.apply(u, y);
            apply$r21
          });
          guard({
            val x$r1 = check$ifrefutable$1._1;
            val z$r1 = check$ifrefutable$3._1;
            val `_ == _$r2` : this.`_ == _`.type = this.`_ == _`;
            val apply$r22 = `_ == _$r2`.apply(x$r1, z$r1);
            apply$r22
          });
          guard({
            val v$r1 = check$ifrefutable$2._2;
            val w = check$ifrefutable$3._2;
            val `_ == _$r3` : this.`_ == _`.type = this.`_ == _`;
            val apply$r23 = `_ == _$r3`.apply(v$r1, w);
            apply$r23
          });
          head[(Int, Int, Int)]({
            val x$r1 = check$ifrefutable$1._1;
            val u = check$ifrefutable$1._2;
            val v$r1 = check$ifrefutable$2._2;
            val apply$r24 = (x$r1, u, v$r1);
            apply$r24
          })
        });
        val app$Alg2$r1 = triangles.fold(alg$Alg2$r1);
        val r1 = app$Alg2$r1._1;
        val r2 = app$Alg2$r1._2;
        val apply$r25 = (r1, r2);
        apply$r25
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "cata-fusion" - {
    "of depth one" - {
      // Note that without CSE, `this.rands` is considered different in every instance.
      val src = u.reify {
        val rds1 = rands
        val rds2 = rands
        val rds3 = rands
        val res1 = rds1.map(`square`).count(`is prime`)
        val res2 = rds2.withFilter(`_ > 0`).min
        val res3 = rds3.flatMap(`+/- 5`).sum
        val res4 = (res1, res2, res3)
        res4
      }.tree

      "in Emma LNF" in {
        val exp = anfPipeline(reify {
          val alg$Min$r2 = alg.Min(Ordering.Int)
          val alg$Sum$r2 = alg.Sum(Numeric.IntIsIntegral)
          val square$r1: this.square.type = this.square
          val `is prime$r1`: this.`is prime`.type = this.`is prime`
          val `_ > 0$r2`: this.`_ > 0`.type = this.`_ > 0`
          val `+/- 5$r1`: this.`+/- 5`.type = this.`+/- 5`
          val rds1 = this.rands
          val rds2 = this.rands
          val rds3 = this.rands
          val alg$Count$r2 = alg.Count(`is prime$r1`)
          val alg$FlatMap$r1 = alg.FlatMap(`+/- 5$r1`, alg$Sum$r2)
          val alg$WithFilter$r1 = alg.WithFilter(`_ > 0$r2`, alg$Min$r2)
          val alg$Map$r1 = alg.Map(square$r1, alg$Count$r2)
          val app$Min$r2 = rds2 fold alg$WithFilter$r1
          val res3 = rds3 fold alg$FlatMap$r1
          val res1 = rds1 fold alg$Map$r1
          val res2 = app$Min$r2.get
          val res4 = (res1, res2, res3)
          res4
        })

        testPipeline(Core.anf)(src) shouldBe alphaEqTo(exp)
      }

      "in Emma Core" in {
        val exp = anfPipeline(reify {
          val alg$Min$r3 = alg.Min(scala.math.Ordering.Int)
          val alg$Sum$r3 = alg.Sum(scala.math.Numeric.IntIsIntegral)
          val square$r3: this.square.type = this.square
          val `is prime$r3` : this.`is prime`.type = this.`is prime`
          val `_ > 0$r4` : this.`_ > 0`.type = this.`_ > 0`
          val `+/- 5$r3` : this.`+/- 5`.type = this.`+/- 5`
          val rds1 = this.rands
          val rds2 = this.rands
          val rds3 = this.rands
          val alg$Count$r3 = alg.Count(`is prime$r3`)
          val alg$WithFilter$r2 = alg.WithFilter(`_ > 0$r4`, alg$Min$r3)
          val fun$FlatMap$r1 = (x$r6: scala.Int) => {
            val xs$r1 = comprehension[scala.Int, DataBag]({
              val x$r12 = generator[scala.Int, DataBag]({
                val x$r13 = `+/- 5$r3`(x$r6)
                x$r13
              })
              head[scala.Int]({
                x$r12
              })
            })
            xs$r1
          }
          val alg$FlatMap$r2 = alg.FlatMap(fun$FlatMap$r1, alg$Sum$r3)
          val alg$Map$r2 = alg.Map(square$r3, alg$Count$r3)
          val app$Min$r3 = rds2.fold(alg$WithFilter$r2)
          val res1 = rds1.fold(alg$Map$r2)
          val res2 = app$Min$r3.get
          val res3 = rds3.fold(alg$FlatMap$r2)
          val res4 = (res1, res2, res3)
          res4
        })

        testPipeline(Core.lift)(src) shouldBe alphaEqTo(exp)
      }
    }

    "of depth two" - {
      val src = u.reify {
        rands.withFilter(`_ > 0`).map(`x => x * x`).sum
      }.tree

      "in Emma LNF" in {
        val exp = anfPipeline(reify {
          val alg$Sum$r3 = alg.Sum(Numeric.IntIsIntegral)
          val rands$r5: this.rands.type = this.rands
          val `_ > 0$r3`: this.`_ > 0`.type = this.`_ > 0`
          val `x => x * x$r1`: this.`x => x * x`.type = this.`x => x * x`
          val alg$Map$r2 = alg.Map(`x => x * x$r1`, alg$Sum$r3)
          val alg$WithFilter$r2 = alg.WithFilter(`_ > 0$r3`, alg$Map$r2)
          val sum$r3 = rands$r5 fold alg$WithFilter$r2
          sum$r3
        })

        testPipeline(Core.anf)(src) shouldBe alphaEqTo(exp)
      }

      "in Emma Core" in {
        val exp = anfPipeline(reify {
          val alg$Sum$r1 = alg.Sum(Numeric.IntIsIntegral)
          val rands$r1: this.rands.type = this.rands
          val `_ > 0$r1` : this.`_ > 0`.type = this.`_ > 0`
          val `x => x * x$r1` : this.`x => x * x`.type = this.`x => x * x`
          val fun$FlatMap$r1 = (x$r1: scala.Int) => {
            val ss$1 = Seq(x$r1)
            val xs$1 = DataBag(ss$1)
            val xs$r1 = comprehension[scala.Int, DataBag]({
              val x$r2 = generator[Int, DataBag]({
                xs$1
              })
              guard({
                val x$r3 = `_ > 0$r1`.apply(x$r2)
                x$r3
              })
              head[scala.Int]({
                val x$r4 = `x => x * x$r1`.apply(x$r2)
                x$r4
              })
            })
            xs$r1
          }
          val alg$FlatMap$r1 = alg.FlatMap(fun$FlatMap$r1, alg$Sum$r1)
          val sum$r1 = rands$r1.fold[scala.Int](alg$FlatMap$r1)
          sum$r1
        })

        testPipeline(Core.lift)(src) shouldBe alphaEqTo(exp)
      }
    }
  }

  "multi-layer fusion" - {
    "without data dependencies" - {
      val src = u.reify {
        /* -- */ val rands = this.rands
        /*  `-- */ val squares = rands.map(`x => x * x`)
        /*    `-- */ val sqSum = squares.sum
        /*    `-- */ val sqPrimes = squares.map(`_ + 1`).count(`is prime`)
        /*  `-- */ val minPos = rands.withFilter(`_ > 0`).min
        (sqSum, sqPrimes, minPos)
      }.tree

      val exp = anfPipeline(reify {
        val alg$Min$r4 = alg.Min(Ordering.Int)
        val alg$Sum$r6 = alg.Sum(Numeric.IntIsIntegral)
        val `x => x * x$r4` : this.`x => x * x`.type = this.`x => x * x`
        val `_ + 1$r1` : this.`_ + 1`.type = this.`_ + 1`
        val `is prime$r4` : this.`is prime`.type = this.`is prime`
        val `_ > 0$r8` : this.`_ > 0`.type = this.`_ > 0`
        val rands = this.rands
        val alg$Count$r4 = alg.Count(`is prime$r4`)
        val alg$WithFilter$r4 = alg.WithFilter(`_ > 0$r8`, alg$Min$r4)
        val alg$Map$r4 = alg.Map(`_ + 1$r1`, alg$Count$r4)
        val alg$Alg2$r1 = alg.Alg2(alg$Map$r4, alg$Sum$r6)
        val alg$Map$r5 = alg.Map(`x => x * x$r4`, alg$Alg2$r1)
        val alg$Alg2$r2 = alg.Alg2(alg$Map$r5, alg$WithFilter$r4)
        val app$Alg2$r2 = rands.fold(alg$Alg2$r2)
        val app$Alg2$r1 = app$Alg2$r2._1
        val app$Min$r4 = app$Alg2$r2._2
        val minPos = app$Min$r4.get
        val sqPrimes = app$Alg2$r1._1
        val sqSum = app$Alg2$r1._2
        val apply$r47 = (sqSum, sqPrimes, minPos)
        apply$r47
      })

      "in Emma LNF" in (testPipeline(Core.anf)(src) shouldBe alphaEqTo(exp))
      "in Emma Core" in (testPipeline(Core.lift)(src) shouldBe alphaEqTo(exp))
    }

    "with data dependencies" - {
      // Note that only `min` and `max` were fused,
      // resp. `map` and the two `counts` were squashed below.
      val src = u.reify {
        /* -- */ val rands: this.rands.type = this.rands
        /*  `-- */ val min = rands.min
        /*  `-- */ val max = rands.max
        /*  `-- */ val scaled = rands.map(x => (x - min) / (max - min).toDouble)
        /*    `-- */ val c1 = scaled.count(`_ <= 0.5`)
        /*    `-- */ val c2 = scaled.count(`_ > 0.5`)
        (c1, c2)
      }.tree

      val exp = anfPipeline(reify {
        val alg$Max$r2 = alg.Max(Ordering.Int)
        val alg$Min$r6 = alg.Min(Ordering.Int)
        val `_ <= 0.5$r1` : this.`_ <= 0.5`.type = this.`_ <= 0.5`
        val `_ > 0.5$r1` : this.`_ > 0.5`.type = this.`_ > 0.5`
        val rands: this.rands.type = this.rands
        val alg$Alg2$r6 = alg.Alg2(alg$Max$r2, alg$Min$r6)
        val alg$Count$r6 = alg.Count(`_ <= 0.5$r1`)
        val alg$Count$r7 = alg.Count(`_ > 0.5$r1`)
        val alg$Alg2$r5 = alg.Alg2(alg$Count$r6, alg$Count$r7)
        val app$Alg2$r6 = rands.fold(alg$Alg2$r6)
        val app$Max$r2 = app$Alg2$r6._1
        val app$Min$r6 = app$Alg2$r6._2
        val max = app$Max$r2.get
        val min = app$Min$r6.get
        val fun$r7 = (x: scala.Int) => {
          val `-$r1` = x - min
          val `-$r2` = max - min
          val toDouble$r1 = `-$r2`.toDouble
          val `/$r1` = `-$r1` / toDouble$r1
          `/$r1`
        }
        val alg$Map$r8 = alg.Map(fun$r7, alg$Alg2$r5)
        val app$Alg2$r5 = rands.fold(alg$Map$r8)
        val c1 = app$Alg2$r5._1
        val c2 = app$Alg2$r5._2
        val apply$r49 = scala.Tuple2(c1, c2)
        apply$r49
      })

      "in Emma LNF" in (testPipeline(Core.anf)(src) shouldBe alphaEqTo(exp))
      "in Emma Core" in (testPipeline(Core.lift)(src) shouldBe alphaEqTo(exp))
    }
  }
}
