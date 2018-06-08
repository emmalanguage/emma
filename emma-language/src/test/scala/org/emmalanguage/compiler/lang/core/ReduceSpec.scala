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

/** A spec for the `ANF.transform` transformation. */
class ReduceSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val expPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lnf
    ).compose(_.tree)

  val actPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lift,
      Reduce.transform.timed
    ).compose(_.tree)

  "propagates trivial value definitions" in {
    val act = actPipeline(reify {
      val xs = this.xs
      val ys = xs.collect()
      val a = Math.PI
      val b = a
      val c = b
      val f = (y: Int) => {
        val s = c
        val t = s
        val u = t * y
        u
      }
      val C = scala.collection.Seq.canBuildFrom[scala.Double]
      val r = ys.map(f)(C)
      r
    })

    val exp = expPipeline(reify {
      val xs = this.xs
      val ys = xs.collect()
      val f = (y: Int) => {
        val u = Math.PI * y
        u
      }
      val C = scala.collection.Seq.canBuildFrom[scala.Double]
      val r = ys.map(f)(C)
      r
    })

    act shouldBe alphaEqTo(exp)
  }

  "does not eliminate implicit ValDefs" in {
    val act = actPipeline(reify {
      val xs = this.xs
      implicit val i = 8
      xs
    })

    val exp = expPipeline(reify {
      val xs = this.xs
      implicit val i = 8
      xs
    })

    act shouldBe alphaEqTo(exp)
  }

  "inlines" - {
    "lambdas in the enclosing let block" in {
      val act = actPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val z = x - y
          val b = z * z
          b
        }
        val a = f(x, 42)
        a
      })

      val exp = expPipeline(reify {
        val x = this.x
        val z = x - 42
        val a = z * z
        a
      })

      act shouldBe alphaEqTo(exp)
    }

    "lambdas in an ancestor let block" in {
      val act = actPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val z = x - y
          val b = z * z
          b
        }
        var a = 0
        if (x > 42) a = f(x, 42)
        a
      })

      val exp = expPipeline(reify {
        val x = this.x
        var a = 0
        if (x > 42) {
          val z = x - 42
          a = z * z
        }
        a
      })

      act shouldBe alphaEqTo(exp)
    }

    "a combination of the two" in {
      val act = actPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val u = x
          val v = y
          val z = u - v
          val b = z * z
          b
        }
        val h = (x: Int, y: Int) => {
          val t = x + y
          val p = Math.PI
          val c = Math.pow(t, p)
          c
        }
        var a = 0
        if (x > 42) a = f(x, 42)
        val b = h(a, 17)
        b
      })

      val exp = expPipeline(reify {
        val x = this.x
        var a = 0
        if (x > 42) {
          val z = x - 42
          a = z * z
        }
        val z = a + 17
        val b = Math.pow(z, Math.PI)
        b
      })

      act shouldBe alphaEqTo(exp)
    }

    "nested lambdas" in {
      val act = actPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val h = (x: Int, y: Int) => {
            val t = x + y
            val p = Math.PI
            val c = Math.pow(t, p)
            c
          }
          val u = x
          val v = y
          val z = u - v
          val b = h(17, z)
          b
        }
        val a = f(x, 42)
        a
      })

      val exp = expPipeline(reify {
        val x = this.x
        val z = x - 42
        val t = 17 + z
        val a = Math.pow(t, Math.PI)
        a
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "does not inline" - {
    "lambdas with control flow" in {
      val act = actPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val z = x - y
          val r = if (z > 0) -1 else 1
          r
        }
        val a = f(x, 42)
        a
      })

      val exp = expPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val z = x - y
          val r = if (z > 0) -1 else 1
          r
        }
        val a = f(x, 42)
        a
      })

      act shouldBe alphaEqTo(exp)
    }

    "lambdas used more than once" in {
      val act = actPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val z = x - y
          val b = z * z
          b
        }
        val a = f(x, 42)
        val b = f(x, 17)
        val c = a + b
        c
      })

      val exp = expPipeline(reify {
        val x = this.x
        val f = (x: Int, y: Int) => {
          val z = x - y
          val b = z * z
          b
        }
        val a = f(x, 42)
        val b = f(x, 17)
        val c = a + b
        c
      })

      act shouldBe alphaEqTo(exp)
    }
  }
}
