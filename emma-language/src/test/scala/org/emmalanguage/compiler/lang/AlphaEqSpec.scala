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
package compiler.lang

import compiler.BaseCompilerSpec

/** A spec for alpha equivalence of Scala ASTs. */
class AlphaEqSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  "simple valdefs and expressions" in {
    val lhs = idPipeline(reify {
      val a$01 = 42 * x
      val a$02 = a$01 * t._1
      15 * a$01 * a$02
    })

    val rhs = idPipeline(reify {
      val b$01 = 42 * x
      val b$02 = b$01 * t._1
      15 * b$01 * b$02
    })

    lhs shouldBe alphaEqTo(rhs)
  }

  "conditionals" in {
    val lhs = idPipeline(reify {
      val a$01 = 42 * x
      if (x < 42) x * t._1 else x / a$01
    })

    val rhs = idPipeline(reify {
      val b$01 = 42 * x
      if (x < 42) x * t._1 else x / b$01
    })

    lhs shouldBe alphaEqTo(rhs)
  }

  "variable assignment and loops" in {
    val lhs = idPipeline(reify {
      var u = x
      while (u < 20) {
        println(y)
        u = u + 1
      }

      do {
        u = u - 2
      } while (u < 20)
    })

    val rhs = idPipeline(reify {
      var v = x
      while (v < 20) {
        println(y)
        v = v + 1
      }

      do {
        v = v - 2
      } while (v < 20)
    })

    lhs shouldBe alphaEqTo(rhs)
  }

  "loops" in {
    val lhs = idPipeline(reify {
      def b$00(): Unit = {
        val i$1 = 0
        val r$1 = 0
        def b$01$while(i: Int, r: Int): Int = {
          def b$01$body(): Int = {
            val i$2 = i + 1
            val r$2 = r * i$2
            b$01$while(i$2, r$2)
          }
          def b$02(i: Int, r: Int): Int = {
            i * r
          }
          if (i < x) b$01$body()
          else b$02(i, r)
        }
        b$01$while(i$1, r$1)
      }
      b$00()
    })

    val rhs = idPipeline(reify {
      def x$00(): Unit = {
        val j$1 = 0
        val k$1 = 0
        def x$01$while(j: Int, k: Int): Int = {
          def x$01$body(): Int = {
            val j$2 = j + 1
            val k$2 = k * j$2
            x$01$while(j$2, k$2)
          }
          def x$02(i: Int, j: Int): Int = {
            i * j
          }
          if (j < x) x$01$body()
          else x$02(j, k)
        }
        x$01$while(j$1, k$1)
      }
      x$00()
    })

    lhs shouldBe alphaEqTo(rhs)
  }

  "pattern matching" in {
    val lhs = idPipeline(reify {
      val u = (t, x)
      u match {
        case ((i, _: String), _) => i * 42
      }
    })

    val rhs = idPipeline(reify {
      val v = (t, x)
      v match {
        case ((l, _: String), _) => l * 42
      }
    })

    lhs shouldBe alphaEqTo(rhs)
  }
}
