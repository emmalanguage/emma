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

import eu.stratosphere.emma.api.DataBag
import compiler.BaseCompilerSpec
import compiler.ir.ComprehensionSyntax._
import compiler.ir.ComprehensionCombinators._
import testschema.Marketing._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for comprehension combination. */
@RunWith(classOf[JUnitRunner])
class CombinationSpec extends BaseCompilerSpec {

  import compiler._
  import compiler.Combination._

  // ---------------------------------------------------------------------------
  // Transformation pipelines
  // ---------------------------------------------------------------------------

  val liftPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf
    ).compose(_.tree)

  val combine: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      tree => time(Comprehension.combine(tree), "combine"),
      Core.flatten
    ).compose(_.tree)

  def applyOnce(rule: u.Tree =?> u.Tree): u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf,
      tree => time(api.TopDown.transform(rule)(tree).tree, "match rule"),
      Core.flatten
    ).compose(_.tree)

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  "apply once " - {

    "MatchFilter" in {

      val inp = u.reify {
        comprehension[User, DataBag] {
          val u = generator[User, DataBag] {
            val f$1 = (u: User) => {
              val x$4 = u.id
              val x$5 = x$4 > 0
              x$5
            }
            val ir$1 = users.withFilter(f$1)
            ir$1
          }
          guard {
            val x$1 = u.id
            val x$2 = x$1 < 11
            x$2
          }
          head {
            u
          }
        }
      }

      val exp = u.reify {
        comprehension[User, DataBag] {
          val u = generator[User, DataBag] {
            val f$1 = (u: User) => {
              val x$4 = u.id
              val x$5 = x$4 > 0
              x$5
            }
            val ir$1 = users.withFilter(f$1)
            val f$2 = (u: User) => {
              val x$1 = u.id
              val x$2 = x$1 < 11
              x$2
            }
            val ir$2 = ir$1.withFilter(f$2)
            ir$2
          }
          head {
            u
          }
        }
      }

      applyOnce(MatchFilter)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "MatchFlatMap" in {

      val users$1 = users

      val inp = u.reify {
        comprehension[Long, DataBag] {
          val x = generator[User, DataBag] {
            users$1
          }
          val y = generator[Long, DataBag] {
            val ir1 = x.id
            val ir2 = Seq(ir1, ir1)
            val ir3 = DataBag(ir2)
            ir3
          }
          head {
            y
          }
        }
      }

      val exp = u.reify {
        comprehension[Long, DataBag] {
          val y = generator[Long, DataBag] {
            val f = (fx: User) => {
              val ir1 = fx.id
              val ir2 = Seq(ir1, ir1)
              val ir3 = DataBag(ir2)
              ir3
            }
            val ir4 = users$1 flatMap f
            ir4
          }
          head {
            y
          }
        }
      }

      applyOnce(MatchFlatMap)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "MatchFlatMap2" in {

      val users$1 = users

      val inp = u.reify {
        comprehension[(User, Long), DataBag] {
          val x = generator[User, DataBag] {
            users$1
          }
          val y = generator[Long, DataBag] {
            val ir1 = x.id
            val ir2 = Seq(ir1, ir1)
            val ir3 = DataBag(ir2)
            ir3
          }
          guard {
            x.id != y
          }
          val z = generator[User, DataBag] {
            DataBag(Seq(x))
          }
          head {
            (x, y)
          }
        }
      }

      val exp = u.reify {
        comprehension[(User, Long), DataBag] {
          val xy = generator[(User, Long), DataBag] {
            val f = (fFuncArg: User) => {
              val ir1 = fFuncArg.id
              val ir2 = Seq(ir1, ir1)
              val ir3 = DataBag(ir2)
              val gFunc = (gFuncArg: Long) => {
                new Tuple2(fFuncArg, gFuncArg)
              }
              val ir5 = ir3 map gFunc
              ir5
            }
            val ir4 = users$1 flatMap f
            ir4
          }
          guard {
            xy._1.id != xy._2
          }
          val z = generator[User, DataBag] {
            DataBag(Seq(xy._1))
          }
          head {
            (xy._1, xy._2)
          }
        }
      }

      applyOnce(MatchFlatMap2)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "MatchCross (simple)" in {

      val inp = u.reify {
        comprehension[(User, User), DataBag] {
          val x = generator[User, DataBag] {
            users
          }
          val y = generator[User, DataBag] {
            users
          }
          head {
            (x,y)
          }
        }
      }

      val exp = u.reify {
        comprehension[(User, User), DataBag] {
          val c = generator[(User, User), DataBag] {
            cross(users, users)
          }
          head {
            (c._1,c._2)
          }
        }
      }

      applyOnce(MatchCross)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "MatchCross (complicated)" in {

      val inp = u.reify {
        comprehension[String, DataBag] {
          val u = generator[DataBag[User], DataBag] {
            DataBag(Seq(users, users))
          }
          val x = generator[User, DataBag] {
            u
          }
          val y = generator[Int, DataBag] {
            DataBag(Seq(1,2,3)) plus DataBag(Seq(4,5,6))
          }
          val z = generator[Long, DataBag] {
            DataBag(Seq(x.id + x.id))
          }
          guard {
            y != 0
          }
          head {
            (x.id + y + z).toString
          }
        }
      }

      val exp = u.reify {
        comprehension[String, DataBag] {
          val u = generator[DataBag[User], DataBag] {
            DataBag(Seq(users, users))
          }
          val c = generator[(User, Int), DataBag] {
            cross(u, DataBag(Seq(1,2,3)) plus DataBag(Seq(4,5,6)))
          }
          val z = generator[Long, DataBag] {
            DataBag(Seq(c._1.id + c._1.id))
          }
          guard {
            c._2 != 0
          }
          head {
            (c._1.id + c._2 + z).toString
          }
        }
      }

      applyOnce(MatchCross)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "MatchEquiJoin (simple)" in {

      val users$1 = users

      val inp = u.reify {
        comprehension[(User, User), DataBag] {
          val x = generator[User, DataBag] {
            users$1
          }
          val y = generator[User, DataBag] {
            users$1
          }
          guard {
            x.id == y.id
          }
          head {
            (x,y)
          }
        }
      }

      val exp = u.reify {
        comprehension[(User, User), DataBag] {
          val j = generator[(User, User), DataBag] {
            val k1 = (x: User) => x.id
            val k2 = (y: User) => y.id
            equiJoin(k1, k2)(users$1, users$1)
          }
          head {
            (j._1, j._2)
          }
        }
      }

      applyOnce(MatchEquiJoin)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "MatchEquiJoin (reversed)" in {

      val users$1 = users

      val inp = u.reify {
        comprehension[(User, User), DataBag] {
          val x = generator[User, DataBag] {
            users$1
          }
          val y = generator[User, DataBag] {
            users$1
          }
          guard {
            y.id == x.id // lhs of `==` refers to the second generator, rhs refers to the first
          }
          head {
            (x,y)
          }
        }
      }

      val exp = u.reify {
        comprehension[(User, User), DataBag] {
          val j = generator[(User, User), DataBag] {
            val k1 = (x: User) => x.id
            val k2 = (y: User) => y.id
            equiJoin(k1, k2)(users$1, users$1)
          }
          head {
            (j._1, j._2)
          }
        }
      }

      applyOnce(MatchEquiJoin)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "MatchEquiJoin (not applicable)" in {

      val matchEquiJoinOnce = applyOnce(MatchEquiJoin)

      val inp = u.reify {
        comprehension[(User, User), DataBag] {
          val x = generator[User, DataBag] {
            users
          }
          val y = generator[User, DataBag] {
            users
          }
          guard {
            x.id + y.id == y.id // the lhs of `==` refers to both x and y, so this is not an equiJoin
          }
          head {
            (x,y)
          }
        }
      }

      matchEquiJoinOnce(inp) shouldBe alphaEqTo(liftPipeline(inp))
    }

    "MatchEquiJoin (complicated)" in {

      val inp = u.reify {
        val a = 5
        comprehension[(Short), DataBag] {
          val w = generator[Int, DataBag] {
            DataBag(Seq(8))
          }
          val x = generator[Int, DataBag] {
            DataBag(Seq(1,2,3)) plus DataBag(Seq(4,5,6))
          }
          val y = generator[User, DataBag] {
            users plus users
          }
          guard {
            val aa = a * a
            val b = 8
            val c = x + 2
            y.id * aa + b == aa + x + c
          }
          val z = generator[Long, DataBag] {
            DataBag(Seq(3L, 6L))
          }
          guard {
            z != x && z != y.id
          }
          head {
            (x + y.name.first.length - z).asInstanceOf[Short]
          }
        }
      }

      val exp = u.reify {
        val a: Int = 5
        comprehension[Short, DataBag] {
          val w: Int = generator[Int, DataBag] {
            val apply$1 = Seq(8)
            val apply$2 = DataBag[Int](apply$1)
            apply$2
          }
          val j: (Int, User) = generator[(Int, User), DataBag] {
            val plus$1: DataBag[Int] = DataBag(Seq(1,2,3)) plus DataBag(Seq(4,5,6))
            val plus$2: DataBag[User] = users plus users
            val kx$3 = (kxArg$3: Int) => {
              val aa: Int = a * a
              val c: Int = kxArg$3 + 2
              val p$3: Int = aa + kxArg$3
              val p$4: Int = p$3 + c
              val cast = p$4.asInstanceOf[Long] // This is added if the key functions' return types are not equal
              cast
            }
            val ky$3 = (kyArg$3: User) => {
              val aa$3: Int = a * a
              val b$3: Int = 8
              val id$1$3: Long = kyArg$3.id
              val pr$2$3: Long = id$1$3 * aa$3
              val p$2$3: Long = pr$2$3 + b$3
              val cast = p$2$3.asInstanceOf[Long] // This is added if the key functions' return types are not equal
              cast
            }
            val ir$3: DataBag[(Int, User)] = equiJoin[Int, User, Long](kx$3, ky$3)(plus$1, plus$2)
            ir$3
          }
          val z: Long = generator[Long, DataBag] {
            DataBag(Seq(3L, 6L))
          }
          guard {
            z != j._1 && z != j._2.id
          }
          head[Short] {
            //(j._1 + j._2.name.first.length - z).asInstanceOf[Short]
            val _2$14 = j._2
            val name$6 = _2$14.name
            val first$6 = name$6.first
            val length$6 = first$6.length
            val _1$14 = j._1
            val `+$18` = _1$14 + length$6
            val `-$6` = `+$18` - z
            val asInstanceOf$8 = `-$6`.asInstanceOf[Short]
            asInstanceOf$8
          }
        }
      }

      applyOnce(MatchEquiJoin)(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "residual" in {

      val users$1 = users

      val inp = u.reify {
        comprehension[User, DataBag] {
          val x = generator[User, DataBag] {
            users$1
          }
          head {
            x
          }
        }
      }

      val exp = u.reify {
        users$1 map { x => x }
      }

      applyOnce(MatchResidual)(inp) shouldBe alphaEqTo(liftPipeline(exp))
      combine(inp) shouldBe alphaEqTo(liftPipeline(exp)) // TODO: @ggevay why test this here?
    }
  }

  "combine" - {

    "two crosses" in {

      val users$1 = users

      val inp = u.reify {
        comprehension[(User, User, Int), DataBag] {
          val x = generator[User, DataBag] {
            users minus users$1
          }
          val y = generator[User, DataBag] {
            users$1 plus users
          }
          val z = generator[Int, DataBag] {
            DataBag(Seq(1,2,3))
          }
          head {
            (x,y,z)
          }
        }
      }

      val exp = u.reify {
        cross(
          cross(
            users minus users$1,
            users$1 plus users
          ),
          DataBag(Seq(1, 2, 3))
        ) map {
          c => (c._1._1, c._1._2, c._2)
        }
      }

      combine(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "filter after cross" in {

      val inp = u.reify {
        comprehension[(User, User), DataBag] {
          val x = generator[User, DataBag] {
            users
          }
          val y = generator[User, DataBag] {
            users
          }
          guard {
            x.id != y.id
          }
          head {
            (x,y)
          }
        }
      }

      val exp = u.reify {
        cross(
          users,
          users
        ).withFilter(
          a => a._1.id != a._2.id
        ) map {
          c => (c._1, c._2)
        }
      }

      combine(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "two filters, then cross" in {

      val inp = u.reify {
        comprehension[(User, User), DataBag] {
          val x = generator[User, DataBag] {
            users
          }
          val y = generator[User, DataBag] {
            users
          }
          guard {
            x.id != 3
          }
          guard {
            y.id != 5
          }
          head {
            (x,y)
          }
        }
      }

      val exp = u.reify {
        cross(
          users.withFilter(a => a.id != 3),
          users.withFilter(a => a.id != 5)
        ) map {
          c => (c._1, c._2)
        }
      }

      combine(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }
  }

  "combine nested" - {
    "in head" in {

      val inp = u.reify {
        comprehension[DataBag[User], DataBag] {
          val x = generator[DataBag[User], DataBag] {
            DataBag(Seq(users,users))
          }
          head {
            comprehension[User, DataBag] {
              val y = generator[User, DataBag] {
                x
              }
              head {
                y
              }
            }
          }
        }
      }

      val exp = u.reify {
        val d = DataBag(Seq(users, users))
        val f1 = (f1Arg: DataBag[User]) => {
          val f2 = (f2Arg: User) => {
            f2Arg
          }
          val ir1 = f1Arg map f2
          ir1
        }
        val ir2 = d map f1
        ir2
      }

      combine(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }
  }

  "combine nested" - {

    "complicated" in {

      val users$1 = users

      val inp = u.reify {
        comprehension[DataBag[User], DataBag] {
          val x = generator[DataBag[User], DataBag] {
            DataBag(Seq(users$1,users$1))
          }
          val y = generator[Long, DataBag] {
            val ir1 = comprehension[Long, DataBag] {
              val w = generator[User, DataBag] {
                x plus x
              }
              head {
                w.id
              }
            }
            ir1
          }
          head {
            comprehension[User, DataBag] {
              val z = generator[User, DataBag] {
                x
              }
              guard {
                z.id != y
              }
              head {
                z
              }
            }
          }
        }
      }

      val exp = u.reify {
        val apply$120 = Seq(users$1, users$1)
        val apply$121 = DataBag(apply$120)
        val f$26 = (fArg$26: DataBag[User]) => {
          val plus$9 = fArg$26 plus fArg$26
          val f$30 = (fArg$30: User) => {
            val id$68 = fArg$30.id
            id$68
          }
          val ir1 = plus$9 map f$30
          val g$8 = (gArg$8: Long) => {
            val ir2$8 = new Tuple2(fArg$26, gArg$8)
            ir2$8
          }
          val xy0$8 = ir1 map g$8
          xy0$8
        }
        val ir$58 = apply$121 flatMap f$26
        val f$28 = (fArg$28: (DataBag[User], Long)) => {
          val _1$75 = fArg$28._1
          val p$11 = (pArg$11: User) => {
            val id$69 = pArg$11.id
            val _2$55 = fArg$28._2
            val `!=$32` = id$69 != _2$55
            `!=$32`
          }
          val ir$64 = _1$75 withFilter p$11
          val f$32 = (fArg$32: User) => {
            fArg$32
          }
          val ir$66 = ir$64 map f$32
          ir$66
        }
        val ir$60 = ir$58 map f$28
        ir$60
      }

      combine(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }
  }
}
