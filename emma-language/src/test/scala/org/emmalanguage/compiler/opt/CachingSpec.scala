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

import api.DataBag
import api.backend.LocalOps._
import compiler.BaseCompilerSpec

/** A spec for order disambiguation. */
class CachingSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val liftPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lift
    ).compose(_.tree)

  val addCacheCalls: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lift,
      Caching.addCacheCalls.timed
    ).compose(_.tree)

  "cache DataBag terms" - {

    "used multiple times" in {

      val inp = reify {
        val i = 2
        val xs = DataBag(1 to 5).withFilter(_ % 2 == 0)
        val ys = xs.map(_ + i)
        val zs = xs.map(_ * i)
        ys union zs
      }

      val exp = reify {
        val i = 2
        val xs = cache {
          DataBag(1 to 5).withFilter(_ % 2 == 0)
        }

        val ys = xs.map(_ + i)
        val zs = xs.map(_ * i)
        ys union zs
      }

      addCacheCalls(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "passed as arguments to a loop method" in {

      val inp = reify {
        val i = 2
        val N = 5
        var xs = DataBag(1 to 5)
        for (_ <- 0 to N) xs = xs.map(_ + i)
        xs
      }

      val exp = reify {
        val i = 2
        val N = 5
        var xs = DataBag(1 to 5)
        for (_ <- 0 to N) xs = cache {
          xs.map(_ + i)
        }

        xs
      }

      addCacheCalls(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "referenced in the closure of a loop method" in {

      val inp = reify {
        val N = 5
        var xs = DataBag(1 to 5)
        val ys = DataBag(1 to 5).withFilter(_ % 2 == 0)
        for (_ <- 0 to N) xs = xs union ys
        xs
      }

      val exp = reify {
        val N = 5
        var xs = DataBag(1 to 5)
        val ys = cache {
          DataBag(1 to 5).withFilter(_ % 2 == 0)
        }

        for (_ <- 0 to N)
          xs = cache {
            xs union ys
          }

        xs
      }

      addCacheCalls(inp) shouldBe alphaEqTo(liftPipeline(exp))
    }

    "not referenced once in the suffix of a loop method" in {
      val inp = u.reify {
        val xs = DataBag(1 to 100)
        val ys = xs.map(x => x * x)
        var i = 0
        while (i < 100) {
          println(i)
          i *= i
        }

        ys
      }

      addCacheCalls(inp) shouldBe alphaEqTo(liftPipeline(inp))
    }
  }
}
