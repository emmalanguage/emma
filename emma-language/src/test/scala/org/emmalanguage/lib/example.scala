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
package lib

import api._

import org.example.Foo

@emma.lib
object example extends Arithmetic {

  // ---------------------------------------------------------------------------
  // Examples A, E, G: acyclic call graphs
  // ---------------------------------------------------------------------------

  def plus[A: Numeric](x: A, y: A): A = {
    val n = implicitly[Numeric[A]]
    n.plus(x, y)
  }

  def times[A: Numeric](x: A, y: A): A = {
    val n = implicitly[Numeric[A]]
    n.times(x, y)
  }

  def square[A: Numeric](x: A): A = {
    times(x, x)
  }

  def sameAs[A](x: A, y: A): Map[A, Boolean] = {
    val m = Map.empty[A, Boolean]
    val b = x == y
    m + (x -> b)
  }

  def pow[A: Numeric](x: A, y: Int): A = {
    require(y >= 0, "Exponent must be a non-negative integer")
    val n = implicitly[Numeric[A]]
    var r = n.one
    for (_ <- 0 until y) r = times(r, x)
    r
  }

  def polynom[A: Numeric](a: Arithmetic)(x: A, y: A): A = {
    a.plus(y, a.times(x, x))
  }

  // ---------------------------------------------------------------------------
  // Example B: two hop cycle: f1 -> f2 -> f1
  // ---------------------------------------------------------------------------

  def f1[A: Numeric](x: A, y: A): A = {
    f2(x, y)
  }

  def f2[A: Numeric](x: A, y: A): A = {
    val n = implicitly[Numeric[A]]
    f1(n.times(x, y), n.plus(x, y))
  }

  // ---------------------------------------------------------------------------
  // Example C: higher-order call cycle: g1 -> g2 -> g5 := g4 -> g3 -> g1
  // ---------------------------------------------------------------------------

  def g1[A: Numeric](x: A): A = {
    val n = implicitly[Numeric[A]]
    val g4 = (y: A) => g3(n.times(y, x))
    g2(g4, x)
  }

  def g2[A: Numeric](g5: A => A, x: A): A = {
    val n = implicitly[Numeric[A]]
    n.minus(x, g5(x))
  }

  def g3[A: Numeric](x: A): A = {
    val n = implicitly[Numeric[A]]
    n.plus(x, g1(x))
  }

  // ---------------------------------------------------------------------------
  // Example D: three hop cycle: h1 -> h2 -> h3 -> h1
  // ---------------------------------------------------------------------------

  def h1[A: Numeric](x: A, y: A): A = {
    h2(x, y)
  }

  def h2[A: Numeric](x: A, y: A): A = {
    val n = implicitly[Numeric[A]]
    h3(n.minus(x, y), n.negate(x))
  }

  def h3[A: Numeric](x: A, y: A): A = {
    val n = implicitly[Numeric[A]]
    h1(n.times(x, y), n.abs(y))
  }

  def baz[A: Numeric](x: A, y: A): Foo.Bar.Baz = {
    val n = implicitly[Numeric[A]]
    Foo.Bar.Baz(n.toInt(n.plus(x, y)))
  }

  // ---------------------------------------------------------------------------
  // Example H: parametric method with parametric parameter
  // ---------------------------------------------------------------------------

  def zipWithZero[A](xs: Seq[A]): Seq[(A, Int)] = {
    xs.map(x => (x, 0))
  }
}
