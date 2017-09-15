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
package compiler.lib

import compiler.BaseCompilerSpec

/** A spec for the `Beta.reduce` transformation. */
trait LibExamples extends BaseCompilerSpec {

  import compiler._
  import u.reify

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
    reify {
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
    reify {
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
    reify {
      val check = (e: Double) => e.<(this.ν);
      val x = 51L;
      val y = 17;
      val e = {
        val x$r1 = {
          val x$r2 = x;
          val y$r2 = y.toLong;
          val evidence$2$r1 = Numeric.LongIsIntegral;
          val n$r2 = Predef.implicitly[scala.math.Numeric[Long]](evidence$2$r1);
          n$r2.times(x$r2, y$r2)
        }.toDouble;
        val y$r1 = {
          val x$r3 = this.μ;
          val evidence$3$r1 = Numeric.DoubleIsFractional;
          {
            val x$r4 = x$r3;
            val y$r3 = x$r3;
            val evidence$2$r2 = evidence$3$r1;
            val n$r3 = Predef.implicitly[scala.math.Numeric[Double]](evidence$2$r2);
            n$r3.times(x$r4, y$r3)
          }
        };
        val evidence$1$r1 = Numeric.DoubleIsFractional;
        val n$r1 = Predef.implicitly[scala.math.Numeric[Double]](evidence$1$r1);
        n$r1.plus(x$r1, y$r1)
      };
      val r = check.apply(e);
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
    reify {
      f1(μ, ν)
    }
  }

  lazy val `Example B (Emma Source)` = {
    import lib.example._
    reify {
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
    reify {
      g1(μ + ν)
    }
  }

  lazy val `Example C (Emma Source)` = {
    import lib.example._
    reify {
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
    reify {
      baz(μ, ν).x + h1(μ, μ)
    }
  }

  lazy val `Example D (Emma Source)` = {
    import lib.example._
    reify {
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
    reify {
      val x = μ * ν
      val y = 100
      val r = pow(x, y)
      r
    }
  }

  lazy val `Example E (Emma Source)` = {
    import lib.example._
    reify {
      val x = μ * ν
      val y = 100
      val r = pow[Double](x, y)(Numeric.DoubleIsFractional)
      r
    }
  }

  lazy val `Example E (normalized)` = {
    reify {
      val x = μ * ν
      val y = 100
      val r = {
        val x$1 = x
        val y$1 = y
        val evidence$1 = Numeric.DoubleIsFractional;
        require(y$1 >= 0, "Exponent must be a non-negative integer")
        val n = implicitly[Numeric[Double]](evidence$1)
        var r = n.one
        for (_ <- 0 until y$1) r = {
          val x$2 = r
          val y$2 = x$1
          val evidence$2 = evidence$1
          val n$1 = implicitly[Numeric[Double]](evidence$2)
          n$1.times(x$2, y$2)
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
    reify {
      val r = sameAs(μ, ν)
      r
    }
  }


  lazy val `Example F (Emma Source)` = {
    import lib.example._
    reify {
      val r = sameAs[Double](μ, ν.toDouble)
      r
    }
  }

  lazy val `Example F (normalized)` = {
    reify {
      val r = {
        val x$1 = this.μ;
        val y$1 = this.ν.toDouble;
        val m = Map.empty[Double, Boolean]
        val b = x$1 == y$1
        m + (x$1 -> b)
      }
      r
    }
  }

  lazy val `Example F (Emma Core)` =
    liftPipeline(`Example F (Original Expr)`)

  // ---------------------------------------------------------------------------
  // Example G: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example G (Original Expr)` = {
    import lib.example._
    reify {
      val r = polynom(lib.example)(μ, 42.42)
      r
    }
  }

  lazy val `Example G (Emma Source)` = {
    import lib.example._
    reify {
      val r = polynom[Double](lib.example)(μ, 42.42)(Numeric.DoubleIsFractional)
      r
    }
  }

  lazy val `Example G (normalized)` = {
    reify {
      val r = {
        val a$1 = lib.example;
        val x$1 = this.μ;
        val y$1 = 42.42;
        val evidence$1 = Numeric.DoubleIsFractional;
        {
          val x$2 = y$1
          val y$2 = {
            val x$3 = x$1
            val y$3 = x$1
            val evidence$3 = evidence$1
            val n$2 = implicitly[Numeric[Double]](evidence$3)
            n$2.times(x$3, y$3)
          }
          val evidence$2 = evidence$1
          val n$1 = implicitly[Numeric[Double]](evidence$2)
          n$1.plus(x$2, y$2)
        }
      }
      r
    }
  }

  lazy val `Example G (Emma Core)` =
    liftPipeline(`Example G (Original Expr)`)

  // ---------------------------------------------------------------------------
  // Example H: Program representations
  // ---------------------------------------------------------------------------

  lazy val `Example H (Original Expr)` = {
    import lib.example._
    reify {
      val xs = Seq('a' -> 1)
      val rs = zipWithZero(xs)
      rs
    }
  }

  lazy val `Example H (Emma Source)` = {
    import lib.example._
    reify {
      val xs = Seq('a' -> 1)
      val rs = zipWithZero(xs)
      rs
    }
  }

  lazy val `Example H (normalized)` = {
    reify {
      val xs = Seq('a' -> 1)
      val rs = {
        val ys = xs
        ys.map(x => (x, 0))
      }
      rs
    }
  }

  lazy val `Example H (Emma Core)` =
    liftPipeline(`Example H (Original Expr)`)
}
