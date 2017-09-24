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
import lib.ml.feature.FeatureSpec

class StatsSpec extends FeatureSpec {

  val rand = util.RanHash(4531401965431L)
  val nDim = 100
  val nEls = 1000

  val seq1: Seq[DVector] = {
    val cols = (0 until nDim).map(j => shuffle(nEls)(rand).map(x => x - 1 + j * nEls)).toArray
    val rows = (0 until nEls).map(i => (0 until nDim).map(j => cols(j)(i).toDouble).toArray)
    rows.map(row => dense(row))
  }

  "summarize" should "compute correct basic stats" in {
    val (act1, act2, act3, act4) = basic(seq1)

    val (exp1, exp2, exp3, exp4) = (
      nEls,
      dense((0 until nDim + 0).map(j => sum(j)).toArray),
      dense((0 until nDim + 0).map(j => j * nEls.toDouble).toArray),
      dense((1 until nDim + 1).map(j => j * nEls.toDouble - 1).toArray)
    )

    act1 shouldEqual exp1
    act2 shouldEqual exp2
    act3 shouldEqual exp3
    act4 shouldEqual exp4
  }

  it should "compute correct moments" in {
    val (act1, act2, act3) = moments(seq1)

    val (exp1, exp2, exp3) = (
      dense((0 until nDim + 0).map(j => mean(j)).toArray),
      dense((0 until nDim + 0).map(j => variance(j)).toArray),
      dense((0 until nDim + 0).map(j => stddev(j)).toArray)
    )

    act1 shouldEqual exp1
    act2 shouldEqual exp2
    act3 shouldEqual exp3
  }

  protected def basic(xs: Seq[DVector]) =
    summarize(
      stat.count,
      stat.sum(nDim),
      stat.min(nDim),
      stat.max(nDim)
    )(DataBag(xs))

  protected def moments(xs: Seq[DVector]) =
    summarize(
      stat.mean(nDim),
      stat.variance(nDim),
      stat.stddev(nDim)
    )(DataBag(xs))

  private def sum(j: Int): Double =
    (j * nEls until (j + 1) * nEls).sum.toDouble

  private def mean(j: Int): Double =
    sum(j) / nEls

  private def variance(j: Int): Double = {
    val mu = mean(j)
    (j * nEls until (j + 1) * nEls).map(x => (x - mu) * (x - mu)).sum / nEls
  }

  private def stddev(j: Int): Double =
    math.sqrt(variance(j))
}
