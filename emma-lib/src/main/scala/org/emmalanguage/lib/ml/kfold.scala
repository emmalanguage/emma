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
package lib.ml

import api._

@emma.lib
object kfold {

  case class Assigned[A](foldID: Int, entry: A)

  /** Assign each entry in `xs` to `split` in the [0, K) range with probability equal to the given `pdf`. */
  def split[A: Meta](fractions: Seq[Double])(xs: DataBag[A])(seed: Long = 631431513L): DataBag[Assigned[A]] = {
    // normalize fractions
    val fsum = fractions.sum
    val pdf = fractions.map(x => x / fsum)
    assert(pdf.sum == 1.0, "Fractions should add up to 1")
    assert(pdf.forall(x => x > 0 && x < 1.0), "Fractions should be in the (0, 1) range.")

    val cdf = pdf.scanLeft(0.0)(_ + _).toArray

    for ((entry, i) <- xs.zipWithIndex()) yield {
      val foldID = indexOf(cdf, util.RanHash(seed).at(i).next())
      Assigned(foldID, entry)
    }
  }

  /** Return entries assigned to all folds except `k`. */
  def except[A: Meta](k: Int)(splits: DataBag[Assigned[A]]): DataBag[A] =
    for (Assigned(split, entry) <- splits if split != k) yield entry

  /** Return entries assigned to fold `k`. */
  def select[A: Meta](k: Int)(splits: DataBag[Assigned[A]]): DataBag[A] =
    for (Assigned(split, entry) <- splits if split == k) yield entry

  /** Return the greatest index `i` of an entry in `cdf` such that `cdf(i) <= x` */
  val indexOf = (cdf: Array[Double], x: Double) => {
    var min = 0
    var max = cdf.length
    while (min != max) {
      val mid = (min + max) / 2
      if (cdf(mid) <= x) min = mid + 1
      else max = mid
    }
    if (min > 0) min - 1
    else min
  }
}
