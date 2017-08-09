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
package lib.ml.feature

import api._
import lib.linalg._

import collection.Map
import scala.collection.breakOut

@emma.lib
object encode {

  val card = 1 << 10

  val native = (x: Any) => x.hashCode()

  def freq[A](N: Int = card, h: A => Int = native)(xs: Array[A]): SVector =
    encode(N, h, (i: Int, F: Map[Int, Double]) => F.getOrElse(i, 0.0) + 1.0)(xs)

  def freq[A](dict: Map[A, Int])(xs: Array[A]): SVector =
    encode.freq(dict.size, dict.apply)(xs) // TODO does not work without target

  def bin[A](N: Int = card, h: A => Int = native)(xs: Array[A]): SVector =
    encode(N, h, (_: Int, _: Map[Int, Double]) => 1.0)(xs)

  def bin[A](dict: Map[A, Int])(xs: Array[A]): SVector =
    encode.bin(dict.size, dict.apply)(xs) // TODO does not work without target

  def dict[A](xs: DataBag[A]): Map[A, Int] =
    xs.distinct.collect().zipWithIndex[A, Map[A, Int]](breakOut)

  def apply[A](
    N: Int = card,
    h: A => Int = native,
    u: (Int, Map[Int, Double]) => Double
  )(xs: Array[A]): SVector = {
    var F = Map.empty[Int, Double] // frequencies map
    val I = index(N, h) _

    val L = xs.length
    var i = 0
    while (i < L) {
      val x = xs(i)
      val y = I(x)
      F += y -> u(y, F)
      i += 1
    }

    sparse(N, F.toSeq)
  }

  def index[A](dict: Map[A, Int])(x: A): Int =
    encode.index(dict.size, dict.apply)(x) // TODO does not work without target

  def index[A](N: Int, h: A => Int)(x: A): Int =
    nonNegativeMod(h(x), N)
}
