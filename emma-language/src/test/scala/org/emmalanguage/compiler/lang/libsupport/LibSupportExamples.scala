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
package compiler.lang.libsupport

import compiler.BaseCompilerSpec

import test.schema.Literature.Book

/** A spec for the `Beta.reduce` transformation. */
trait LibSupportExamples extends BaseCompilerSpec {

  import compiler._

  val liftPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lift
    ) andThen unQualifyStatics compose (_.tree)

  val prettyPrint: u.Tree => String =
    tree => time(Core.prettyPrint(tree), "pretty print")

  // ---------------------------------------------------------------------------
  // Program closure
  // ---------------------------------------------------------------------------

  val μ = 3.14
  val ν = 42

  // ---------------------------------------------------------------------------
  // Example A: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example A (Original Expr)` = {
    import lib.example._
    u.reify {
      val check = (e: Double) => e < ν
      val x = 51L
      val y = 17
      val e = plus(times(x, y), square(μ))
      val r = check(e)
      r
    }
  }

  lazy val `Example A (Emma Source)` = {
    import lib.example._
    u.reify {
      val check = (e: Double) => e < ν
      val x = 51L
      val y = 17
      val e = plus[Double](
        times[Long](x, y.toLong)(Numeric.LongIsIntegral).toDouble,
        square[Double](μ)(Numeric.DoubleIsFractional)
      )(Numeric.DoubleIsFractional)
      val r = check(e)
      r
    }
  }

  lazy val `Example A (normalized)` = {
    u.reify {
      val check = (e: Double) => e < ν
      val x = 51L
      val y = 17
      val e = {
        val n$2 = implicitly[Numeric[Double]](Numeric.DoubleIsFractional)
        n$2.plus(
          {
            val n = implicitly[Numeric[Long]](Numeric.LongIsIntegral)
            n.times(x, y.toLong)
          }.toDouble, {
            val x$1 = μ;
            {
              val n = implicitly[Numeric[Double]](Numeric.DoubleIsFractional)
              n.times(x$1, x$1)
            }
          }
        )
      }
      val r = check(e)
      r
    }
  }

  lazy val `Example A (Emma Core)` =
    liftPipeline(`Example A (Original Expr)`)

  // ---------------------------------------------------------------------------
  // Example B: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example B (Original Expr)` = {
    import lib.example._
    u.reify {
      f1(μ, ν)
    }
  }

  lazy val `Example B (Emma Source)` = {
    import lib.example._
    u.reify {
      f1[Double](μ, ν.toDouble)(Numeric.DoubleIsFractional)
    }
  }

  lazy val `Example B (Emma Core)` =
    liftPipeline(`Example B (Original Expr)`)

  // ---------------------------------------------------------------------------
  // Example C: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example C (Original Expr)` = {
    import lib.example._
    u.reify {
      g1(μ + ν)
    }
  }

  lazy val `Example C (Emma Source)` = {
    import lib.example._
    u.reify {
      g1[Double](μ + ν)(Numeric.DoubleIsFractional)
    }
  }

  lazy val `Example C(Emma Core)` =
    liftPipeline(`Example C (Original Expr)`)

  // ---------------------------------------------------------------------------
  // Example D: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example D (Original Expr)` = {
    import lib.example._
    u.reify {
      baz(μ, ν).x + h1(μ, μ)
    }
  }

  lazy val `Example D (Emma Source)` = {
    import lib.example._
    u.reify {
      baz[Double](μ, ν.toDouble)(Numeric.DoubleIsFractional).x +
        h1[Double](μ, μ)(Numeric.DoubleIsFractional)
    }
  }

  lazy val `Example D (Emma Core)` =
    liftPipeline(`Example D (Original Expr)`)

  // ---------------------------------------------------------------------------
  // Example E: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example E (Original Expr)` = {
    import lib.example._
    u.reify {
      val x = μ * ν
      val y = 100
      val r = pow(x, y)
      r
    }
  }

  lazy val `Example E (Emma Source)` = {
    import lib.example._
    u.reify {
      val x = μ * ν
      val y = 100
      val r = pow[Double](x, y)(Numeric.DoubleIsFractional)
      r
    }
  }

  lazy val `Example E (normalized)` = {
    u.reify {
      val x = μ * ν
      val y = 100
      val r = {
        val y$1 = y
        val evidence$4: Numeric[Double] = Numeric.DoubleIsFractional;
        require(y$1 >= 0, "Exponent must be a non-negative integer")
        val n = implicitly[Numeric[Double]](evidence$4)
        var r = n.one
        for (_ <- 0 until y$1) r = {
          val n$1 = implicitly[Numeric[Double]](evidence$4)
          n$1.times(r, x)
        }
        r
      }
      r
    }
  }

  lazy val `Example E (Emma Core)` =
    liftPipeline(`Example E (Original Expr)`)

  // ---------------------------------------------------------------------------
  // Example F: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example F (Original Expr)` = {
    import lib.example._
    u.reify {
      val r = sameAs(μ, ν)
      r
    }
  }

  lazy val `Example F (Emma Source)` = {
    import lib.example._
    u.reify {
      val r = sameAs[Double](μ, ν.toDouble)
      r
    }
  }

  lazy val `Example F (normalized)` = {
    u.reify {
      val r = {
        μ == ν.toDouble
      }
      r
    }
  }

  lazy val `Example F (Emma Core)` =
    liftPipeline(`Example F (Original Expr)`)
}
