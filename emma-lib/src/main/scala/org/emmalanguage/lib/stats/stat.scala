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
package lib.stats

import api._
import lib.linalg._

@emma.lib
object stat {
  def count(xs: DataBag[DVector]): Long =
    xs.size

  def sum(D: Int)(xs: DataBag[DVector]): DVector =
    xs.fold(salg.Sum(D))

  def min(D: Int)(xs: DataBag[DVector]): DVector =
    xs.fold(salg.Min(D))

  def max(D: Int)(xs: DataBag[DVector]): DVector =
    xs.fold(salg.Max(D))

  def mean(D: Int)(xs: DataBag[DVector]): DVector = {
    sum(D)(xs) * (1.0 / count(xs))
  }

  def variance(D: Int)(xs: DataBag[DVector]): DVector = {
    val means = mean(D)(xs)

    val sqrdevss = for (x <- xs) yield {
      val sqrdevs = Array.ofDim[Double](D)
      var i = 0
      while (i < D) {
        val dev = x(i) - means(i)
        sqrdevs(i) = dev * dev
        i += 1
      }
      dense(sqrdevs)
    }

    sum(D)(sqrdevss) * (1.0 / count(xs))
  }

  def stddev(D: Int)(xs: DataBag[DVector]): DVector = {
    val vars = variance(D)(xs)

    val stddevs = Array.ofDim[Double](D)
    var i = 0
    while (i < D) {
      stddevs(i) = math.sqrt(vars(i))
      i += 1
    }
    dense(stddevs)
  }
}
