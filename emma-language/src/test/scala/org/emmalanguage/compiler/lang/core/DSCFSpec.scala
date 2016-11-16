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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `DSCF.lnf` transformation. */
@RunWith(classOf[JUnitRunner])
class DSCFSpec extends BaseCompilerSpec {

  import compiler._

  val dscfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf,
      tree => time(DSCF.transform(tree), "dscf")
    ).compose(_.tree)

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf
    ).compose(_.tree)

  "While Loop" - {
    "with trivial body" in {

      val act = dscfPipeline(u.reify {
        var i = 0
        while (i < 100) {
          i += 1
        }
        println(i)
      })

      val exp = anfPipeline(u.reify {
        val i$1 = 0
        def while$1(i$2: Int): Unit = {
          val x$1 = i$2 < 100
          def body$1(i$3: Int): Unit = {
            val i$2 = i$3 + 1
            while$1(i$2)
          }
          def suffix$1(i$4: Int): Unit = {
            println(i$4)
          }
          if (x$1) body$1(i$2)
          else suffix$1(i$2)
        }
        while$1(i$1)
      })

      act shouldBe alphaEqTo(exp)
    }

    "with non-trivial body" in {

      val act = dscfPipeline(u.reify {
        var i = 0
        while (i < 100) {
          if (i % 2 == 0) i += 2
          else i *= 2
        }
        println(i)
      })

      val exp = anfPipeline(u.reify {
        val i$1 = 0
        def while$1(i$2: Int): Unit = {
          val x$1 = i$2 < 100
          def body$1(i$2: Int): Unit = {
            val x$2 = i$2 % 2
            val x$3 = x$2 == 0
            def then$1(): Unit = {
              val i$3 = i$2 + 2
              suffix$2(i$3)
            }
            def else$1(): Unit = {
              val i$4 = i$2 * 2
              suffix$2(i$4)
            }
            def suffix$2(i$5: Int): Unit = {
              while$1(i$5)
            }
            if (x$3) then$1()
            else else$1()
          }
          def suffix$1(i$6: Int): Unit = {
            println(i$6)
          }
          if (x$1) body$1(i$2)
          else suffix$1(i$2)
        }
        while$1(i$1)
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "Do-While Loop" - {

    "with trivial body" in {

      val act = dscfPipeline(u.reify {
        var i = 0
        do {
          i += 1
        } while (i < 100)
        println(i)
      })

      val exp = anfPipeline(u.reify {
        val i$1 = 0
        def doWhile$1(i$2: Int): Unit = {
          val i$3 = i$2 + 1
          val x$1 = i$3 < 100
          def suffix$1(): Unit = {
            println(i$3)
          }
          if (x$1) doWhile$1(i$3)
          else suffix$1()
        }
        doWhile$1(i$1)
      })

      act shouldBe alphaEqTo(exp)
    }

    "with non-trivial body" in {

      val act = dscfPipeline(u.reify {
        var i = 0
        do {
          if (i % 2 == 0) i += 2
          else i *= 2
        } while (i < 100)
        println(i)
      })

      val exp = anfPipeline(u.reify {
        val i$1 = 0
        def doWhile$1(i$2: Int): Unit = {
          val x$2 = i$2 % 2
          val x$3 = x$2 == 0
          def then$1(): Unit = {
            val i$3 = i$2 + 2
            suffix$2(i$3)
          }
          def else$1(): Unit = {
            val i$4 = i$2 * 2
            suffix$2(i$4)
          }
          def suffix$2(i$5: Int): Unit = {
            val x$1 = i$5 < 100
            def suffix$1(): Unit = {
              println(i$5)
            }
            if (x$1) doWhile$1(i$5)
            else suffix$1()
          }
          if (x$3) then$1()
          else else$1()
        }
        doWhile$1(i$1)
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "Talor Series expansion for sin(x) around x = 0" in {

    val act = dscfPipeline(u.reify {
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

    val exp = anfPipeline(u.reify {
      val sin = (x: Double) => {
        val K = 13
        val xsq = x * x

        val k$1 = 1
        val num$1 = x
        val den$1 = 1

        val S$1 = 0.0

        def B$2(S$2: Double, den$2: Int, k$2: Int, num$2: Double): Double = {
          val frac = num$2 / den$2
          val c$1 = k$2 % 2 == 0
          def B$3(): Double = {
            val S$3 = S$2 - frac
            B$5(S$3)
          }
          def B$4(): Double = {
            val S$4 = S$2 + frac
            B$5(S$4)
          }
          def B$5(S$5: Double): Double = {
            val k$3 = k$2 + 1
            val num$3 = num$2 * xsq
            val den$3 = den$2 * ((2 * k$3 - 2) * (2 * k$3 - 1))
            val c$2 = k$3 < K
            def B$6(): Double = {
              S$5
            }
            if (c$2) B$2(S$5, den$3, k$3, num$3)
            else B$6()
          }
          if (c$1) B$3() else B$4()
        }
        B$2(S$1, den$1, k$1, num$1)
      }

      sin(Math.PI / 2)
    })

    act shouldBe alphaEqTo(exp)
  }

  "Google Code Jam 2015 A1 - Haircut (verified)" in {

    val act = dscfPipeline(u.reify {
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

    val exp = anfPipeline(u.reify {
      implicit val zipSeqWithIdx = Seq.canBuildFrom[(Int, Int)]
      val customers = 4
      val barbers = Seq(10, 5)
      val barber$1 = 0
      val rate = barbers.map(1.0 / _).sum
      val time$1 = ((0 max (customers - barbers.length - 1)) / rate).toLong - 1
      val less$1 = time$1 < 0
      def else$1(): Int = {
        val served$1 = barbers.map(time$1 / _ + 1).sum
        suffix$1(served$1)
      }
      def suffix$1(served$2: Long): Int = {
        val remaining$1 = customers - served$2
        def while$1(barber$2: Int, remaining$2: Long, time$2: Long): Int = {
          val greater$1 = remaining$2 > 0
          def body$1(barber$3: Int, remaining$3: Long, time$3: Long): Int = {
            val time$4 = time$3 + barbers.map(b => b - time$3 % b).min
            val iter$1 = barbers.zipWithIndex.toIterator
            val bi$1 = null.asInstanceOf[(Int, Int)]
            def while$2(barber$4: Int, bi$2: (Int, Int), remaining$4: Long): Int = {
              val hasNext$1 = iter$1.hasNext
              def body$2(barber$5: Int, bi$3: (Int, Int), remaining$5: Long): Int = {
                val bi$4 = iter$1.next()
                val b = bi$4._1
                val i = bi$4._2
                val eq$1 = time$4 % b == 0
                def then$1(): Int = {
                  val remaining$7 = remaining$5 - 1
                  val eq$2 = remaining$7 == 0
                  def then$2(): Int = {
                    val barber$9 = i + 1
                    suffix$2(barber$9)
                  }
                  def suffix$2(barber$10: Int): Int = {
                    suffix$3(barber$10, remaining$7)
                  }
                  if (eq$2) then$2()
                  else suffix$2(barber$5)
                }
                def suffix$3(barber$11: Int, remaining$8: Long): Int = {
                  while$2(barber$11, bi$4, remaining$8)
                }
                if (eq$1) then$1()
                else suffix$3(barber$5, remaining$5)
              }
              def suffix$4(barber$12: Int, remaining$9: Long): Int = {
                while$1(barber$12, remaining$9, time$4)
              }
              if (hasNext$1) body$2(barber$4, bi$2, remaining$4)
              else suffix$4(barber$4, remaining$4)
            }
            while$2(barber$3, bi$1, remaining$3)
          }
          def suffix$5(barber$13: Int): Int = {
            barber$13
          }
          if (greater$1) body$1(barber$2, remaining$2, time$2)
          else suffix$5(barber$2)
        }
        while$1(barber$1, remaining$1, time$1)
      }
      if (less$1) suffix$1(0L) else else$1()
    })

    act shouldBe alphaEqTo(exp)
  }

  "Google Code Jam 2016 Qual - Counting sheep (verified)" in {
    val act = dscfPipeline(u.reify {
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

    val exp = anfPipeline(u.reify {
      val t1 = 1
      val step = 88
      val eq$1 = step == 0
      def then$1(): Unit = {
        val println$1 = println(s"Case #$t1: INSOMNIA")
        suffix$3(println$1)
      }
      def else$1(): Unit = {
        val sheep$1 = step
        val digits$1 = 0
        def while$1(digits$2: Int, sheep$2: Int): Unit = {
          val neq$1 = digits$2 != 1023
          def body$1(digits$3: Int, sheep$3: Int): Unit = {
            val current$1 = sheep$3
            def while$2(current$2: Int, digits$4: Int): Unit = {
              val neq$2 = current$2 != 0
              def body$2(current$3: Int, digits$5: Int): Unit = {
                val digits$6 = digits$5 | (1 << (current$3 % 10))
                val current$4 = current$3 / 10
                while$2(current$4, digits$6)
              }
              def suffix$1(digits$7: Int): Unit = {
                val sheep$4 = sheep$3 + step
                while$1(digits$7, sheep$4)
              }
              if (neq$2) body$2(current$2, digits$4)
              else suffix$1(digits$4)
            }
            while$2(current$1, digits$3)
          }
          def suffix$2(sheep$5: Int): Unit = {
            val sheep$6 = sheep$5 - step
            val println$2 = println(s"Case #$t1: ${sheep$6}")
            suffix$3(println$2)
          }
          if (neq$1) body$1(digits$2, sheep$2)
          else suffix$2(sheep$2)
        }
        while$1(digits$1, sheep$1)
      }
      def suffix$3(println$3: Unit): Unit = println$3
      if (eq$1) then$1() else else$1()
    })

    act shouldBe alphaEqTo(exp)
  }

  "Google Code Jam 2016 Qual - Fractiles (verified)" in {
    val act = dscfPipeline(u.reify {
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
          tile = (tile - one) * tiles +
            tile % tiles + one
        }

        infoGain += level
        check ::= tile
        tile = infoGain + 1
      }

      check.reverse.mkString(" ")
    })

    val exp = anfPipeline(u.reify {
      val (tiles, complexity) = (2, 3)
      val one = BigInt(1)
      val necessary = tiles min complexity
      val infoGain$1 = 0
      val check$1 = List.empty[BigInt]
      val tile$1 = one
      def while$1(check$2: List[BigInt], infoGain$2: Int, tile$2: BigInt): String = {
        val less$1 = infoGain$2 < tiles
        def body$1(check$3: List[BigInt], infoGain$3: Int, tile$3: BigInt): String = {
          val level$1 = 1
          def while$2(level$2: Int, tile$4: BigInt): String = {
            val less$2 = level$2 < necessary
            def body$2(level$3: Int, tile$5: BigInt): String = {
              val level$4 = level$3 + 1
              val tile$6 = (tile$5 - one) * tiles +
                tile$5 % tiles + one
              while$2(level$4, tile$6)
            }
            def suffix$1(level$5: Int, tile$8: BigInt): String = {
              val infoGain$4 = infoGain$3 + level$5
              val check$4 = check$3.::(tile$8)
              val tile$9 = infoGain$4 + 1
              while$1(check$4, infoGain$4, tile$9)
            }
            if (less$2) body$2(level$2, tile$4)
            else suffix$1(level$2, tile$4)
          }
          while$2(level$1, tile$3)
        }
        def suffix$2(check$5: List[BigInt]): String = {
          check$5.reverse.mkString(" ")
        }
        if (less$1) body$1(check$2, infoGain$2, tile$2)
        else suffix$2(check$2)
      }
      while$1(check$1, infoGain$1, tile$1)
    })

    act shouldBe alphaEqTo(exp)
  }
}
