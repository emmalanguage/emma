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
package compiler.lang.core

import compiler.BaseCompilerSpec
import compiler.ir.DSCFAnnotations._

import scala.util.control.TailCalls._

/** A spec for the `Core.trampoline` transformation. */
class TrampolineSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val trampolinePipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf,
      Core.dscf,
      Core.trampoline.timed,
      Core.anf
    ).compose(_.tree)

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf
    ).compose(_.tree)

  "Taylor Series expansion for sin(x) around x = 0" in {
    val act = trampolinePipeline(reify {
      val sin = (x: Double) => {
        val K = 13
        val xsq = x * x
        var k = 1
        var num = x
        var den = 1
        var S = 0.0
        do {
          val frac = num / den
          if (k % 2 == 0) S -= frac
          else S += frac
          k += 1
          num *= xsq
          den *= (2 * k - 2) * (2 * k - 1)
        } while (k < K)
        S
      }
      sin(Math.PI / 2)
    })

    val exp = anfPipeline(reify {
      val sin = (x: Double) => {
        val K = 13
        val xsq = x * x
        @whileLoop def B$2(S$2: Double, den$2: Int, k$2: Int, num$2: Double): TailRec[Double] = {
          val frac = num$2 / den$2
          val c$1 = k$2 % 2 == 0
          @suffix def B$5(S$5: Double): TailRec[Double] = {
            val k$3 = k$2 + 1
            val num$3 = num$2 * xsq
            val den$3 = den$2 * ((2 * k$3 - 2) * (2 * k$3 - 1))
            val c$2 = k$3 < K
            @suffix def B$6(): TailRec[Double] = {
              done(S$5)
            }
            if (c$2) tailcall(B$2(S$5, den$3, k$3, num$3))
            else tailcall(B$6())
          }
          @thenBranch def B$3(): TailRec[Double] = {
            val S$3 = S$2 - frac
            tailcall(B$5(S$3))
          }
          @elseBranch def B$4(): TailRec[Double] = {
            val S$4 = S$2 + frac
            tailcall(B$5(S$4))
          }
          if (c$1) tailcall(B$3()) else tailcall(B$4())
        }
        B$2(0.0, 1, 1, x).result
      }
      val sin$1 = sin(Math.PI / 2)
      sin$1
    })

    act shouldBe alphaEqTo (exp)
  }

  "Google Code Jam 2015 A1 - Haircut (verified)" in {
    val act = trampolinePipeline(reify {
      implicit val zipSeqWithIdx = Seq.canBuildFrom[(Int, Int)]
      val customers = 4
      val barbers = Seq(10, 5)
      var barber = 0
      val rate = barbers.map(1.0 / _).sum
      var time = ((0 max (customers - barbers.length - 1)) / rate).toLong - 1
      val served = if (time < 0) 0 else barbers.map(time / _ + 1).sum
      var remaining = customers - served
      while (remaining > 0) {
        time += barbers.map(b => b - time % b).min
        for (bi <- barbers.zipWithIndex) {
          val b = bi._1
          val i = bi._2
          if (time % b == 0) {
            remaining -= 1
            if (remaining == 0) barber = i + 1
          }
        }
      }

      barber
    })

    val exp = anfPipeline(reify {
      implicit val zipSeqWithIdx = Seq.canBuildFrom[(Int, Int)]
      val customers = 4
      val barbers = Seq(10, 5)
      val rate = barbers.map(1.0 / _).sum
      val time$1 = ((0 max (customers - barbers.length - 1)) / rate).toLong - 1
      val less$1 = time$1 < 0
      @suffix def suffix$1(served$2: Long): TailRec[Int] = {
        val remaining$1 = customers - served$2
        @whileLoop def while$1(barber$2: Int, remaining$2: Long, time$2: Long): TailRec[Int] = {
          val greater$1 = remaining$2 > 0
          @loopBody def body$1(): TailRec[Int] = {
            val time$4 = time$2 + barbers.map(b => b - time$2 % b).min
            val iter$1 = barbers.zipWithIndex.toIterator
            val bi$1 = null.asInstanceOf[(Int, Int)]
            @whileLoop def while$2(barber$4: Int, bi$2: (Int, Int), remaining$4: Long): TailRec[Int] = {
              val hasNext$1 = iter$1.hasNext
              @loopBody def body$2(): TailRec[Int] = {
                val bi$4 = iter$1.next()
                val b = bi$4._1
                val i = bi$4._2
                val eq$1 = time$4 % b == 0
                @suffix def suffix$3(barber$11: Int, remaining$8: Long): TailRec[Int] = {
                  tailcall(while$2(barber$11, bi$4, remaining$8))
                }
                @thenBranch def then$1(): TailRec[Int] = {
                  val remaining$7 = remaining$4 - 1
                  val eq$2 = remaining$7 == 0
                  @suffix def suffix$2(barber$10: Int): TailRec[Int] = {
                    tailcall(suffix$3(barber$10, remaining$7))
                  }
                  @thenBranch def then$2(): TailRec[Int] = {
                    val barber$9 = i + 1
                    tailcall(suffix$2(barber$9))
                  }
                  if (eq$2) tailcall(then$2())
                  else tailcall(suffix$2(barber$4))
                }
                if (eq$1) tailcall(then$1())
                else tailcall(suffix$3(barber$4, remaining$4))
              }
              @suffix def suffix$4(): TailRec[Int] = {
                tailcall(while$1(barber$4, remaining$4, time$4))
              }
              if (hasNext$1) tailcall(body$2())
              else tailcall(suffix$4())
            }
            tailcall(while$2(barber$2, bi$1, remaining$2))
          }
          @suffix def suffix$5(): TailRec[Int] = {
            done(barber$2)
          }
          if (greater$1) tailcall(body$1())
          else tailcall(suffix$5())
        }
        tailcall(while$1(0, remaining$1, time$1))
      }
      @elseBranch def else$1(): TailRec[Int] = {
        val served$1 = barbers.map(time$1 / _ + 1).sum
        tailcall(suffix$1(served$1))
      }
      if (less$1) suffix$1(0L).result
      else else$1().result
    })

    act shouldBe alphaEqTo (exp)
  }

  "Google Code Jam 2016 Qual - Counting sheep (verified)" in {
    val act = trampolinePipeline(reify {
      val t1 = 1
      val step = 88
      if (step == 0) {
        println(s"Case #$t1: INSOMNIA")
      } else {
        var sheep = step
        var digits = 0
        while (digits != 0x3ff) {
          var current = sheep
          while (current != 0) {
            digits |= 1 << (current % 10)
            current /= 10
          }

          sheep += step
        }

        sheep -= step
        println(s"Case #$t1: $sheep")
      }
    })

    val exp = anfPipeline(reify {
      val t1 = 1
      val step = 88
      val eq$1 = step == 0
      @suffix def suffix$3(println$3: Unit): TailRec[Unit] = {
        done(println$3)
      }
      @thenBranch def then$1(): TailRec[Unit] = {
        val println$1 = println(s"Case #$t1: INSOMNIA")
        tailcall(suffix$3(println$1))
      }
      @elseBranch def else$1(): TailRec[Unit] = {
        @whileLoop def while$1(digits$2: Int, sheep$2: Int): TailRec[Unit] = {
          val neq$1 = digits$2 != 1023
          @loopBody def body$1(): TailRec[Unit] = {
            @whileLoop def while$2(current$2: Int, digits$4: Int): TailRec[Unit] = {
              val neq$2 = current$2 != 0
              @loopBody def body$2(): TailRec[Unit] = {
                val digits$6 = digits$4 | (1 << (current$2 % 10))
                val current$4 = current$2 / 10
                tailcall(while$2(current$4, digits$6))
              }
              @suffix def suffix$1(): TailRec[Unit] = {
                val sheep$4 = sheep$2 + step
                tailcall(while$1(digits$4, sheep$4))
              }
              if (neq$2) tailcall(body$2())
              else tailcall(suffix$1())
            }
            tailcall(while$2(sheep$2, digits$2))
          }
          @suffix def suffix$2(): TailRec[Unit] = {
            val sheep$6 = sheep$2 - step
            val println$2 = println(s"Case #$t1: ${sheep$6}")
            tailcall(suffix$3(println$2))
          }
          if (neq$1) tailcall(body$1())
          else tailcall(suffix$2())
        }
        tailcall(while$1(0, step))
      }
      if (eq$1) then$1().result
      else else$1().result
    })

    act shouldBe alphaEqTo (exp)
  }

  "Google Code Jam 2016 Qual - Fractiles (verified)" in {
    val act = trampolinePipeline(reify {
      val (tiles, complexity) = (2, 3)
      val one = BigInt(1)
      val necessary = tiles min complexity
      var infoGain = 0
      var check = List.empty[BigInt]
      var tile = one
      while (infoGain < tiles) {
        var level = 1
        while (level < necessary) {
          level += 1
          tile = (tile - one) * tiles + tile % tiles + one
        }

        infoGain += level
        check ::= tile
        tile = infoGain + 1
      }

      check.reverse.mkString(" ")
    })

    val exp = anfPipeline(reify {
      val (tiles, complexity) = (2, 3)
      val one = BigInt(1)
      val necessary = tiles min complexity
      val check$1 = List.empty[BigInt]
      @whileLoop def while$1(check$2: List[BigInt], infoGain$2: Int, tile$2: BigInt): TailRec[String] = {
        val less$1 = infoGain$2 < tiles
        @loopBody def body$1(): TailRec[String] = {
          @whileLoop def while$2(level$2: Int, tile$4: BigInt): TailRec[String] = {
            val less$2 = level$2 < necessary
            @loopBody def body$2(): TailRec[String] = {
              val level$4 = level$2 + 1
              val tile$6 = (tile$4 - one) * tiles + tile$4 % tiles + one
              tailcall(while$2(level$4, tile$6))
            }
            @suffix def suffix$1(): TailRec[String] = {
              val infoGain$4 = infoGain$2 + level$2
              val check$4 = check$2.::(tile$4)
              val tile$9 = infoGain$4 + 1
              val tile$10: BigInt = tile$9
              tailcall(while$1(check$4, infoGain$4, tile$10))
            }
            if (less$2) tailcall(body$2())
            else tailcall(suffix$1())
          }
          tailcall(while$2(1, tile$2))
        }
        @suffix def suffix$2(): TailRec[String] = {
          done(check$2.reverse.mkString(" "))
        }
        if (less$1) tailcall(body$1())
        else tailcall(suffix$2())
      }
      while$1(check$1, 0, one).result
    })

    act shouldBe alphaEqTo (exp)
  }
}
