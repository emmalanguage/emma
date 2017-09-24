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
package lib.ml.classification

import api._
import lib.linalg._
import lib.ml._

/**
 * Test data is randomly distributed in the unit square.
 * Test labels are actually computed as `y >= 0.5`,
 * while the hypotesis predicts labels as `x <= 0.5`.
 * This leads to the following 4 quadrants around the `(0.5,0.5)` center point:
 *
 * {{{
 *  TP | FN
 *  ---|---
 *  FP | TN
 * }}}
 */
class EvalSpec extends lib.BaseLibSpec {
  /** Labeled point type. */
  type Point = LDPoint[Int, Boolean]

  /** Comparison tolerance. */
  val e = 1e-3 // double tolerance
  /** Hypothesis */
  val h = (pos: DVector) => pos(0) <= 0.5
  /** Number of points. */
  val N = 1000
  /** Random Seed. */
  val S = 35351343215L
  /* Labeled test points in [-1,-1]^2. */
  val ps: Seq[LDPoint[Int, Boolean]] = for (i <- 1 to N) yield {
    val x = util.RanHash(S, 0).at(i).next()
    val y = util.RanHash(S, 1).at(i).next()
    LDPoint(i, dense(Array(x, y)), y >= 0.5)
  }

  "eval.precision" should "compute correct results" in {
    val act = actPrecision(h, ps)
    val exp = expPrecision(ps)
    act shouldEqual (exp +- e)
  }

  "eval.recall" should "compute correct results" in {
    val act = actRecall(h, ps)
    val exp = expRecall(ps)
    act shouldEqual (exp +- e)
  }

  "eval.f1score" should "compute correct results" in {
    val act = actF1Score(h, ps)
    val exp = expF1Score(ps)
    act shouldEqual (exp +- e)
  }

  def actPrecision(h: DVector => Boolean, ps: Seq[Point]) =
    eval.precision(h)(DataBag(ps))

  def actRecall(h: DVector => Boolean, ps: Seq[Point]) =
    eval.recall(h)(DataBag(ps))

  def actF1Score(h: DVector => Boolean, ps: Seq[Point]) =
    eval.f1score(h)(DataBag(ps))

  private def expPrecision(ps: Seq[Point]) = {
    val tp = ps.count(quadrant(_) == eval.TP).toDouble
    val fp = ps.count(quadrant(_) == eval.FP).toDouble
    tp / (tp + fp)
  }

  private def expRecall(seq: Seq[Point]) = {
    val tp = ps.count(quadrant(_) == eval.TP).toDouble
    val fn = ps.count(quadrant(_) == eval.FN).toDouble
    tp / (tp + fn)
  }

  private def expF1Score(ps: Seq[Point]) = {
    val p = expPrecision(ps)
    val r = expRecall(ps)
    2.0 * (p * r) / (p + r)
  }

  private def quadrant(p: Point): Int =
    if (p.pos(1) >= 0.5) {
      if (p.pos(0) <= 0.5) eval.TP
      else eval.FN
    } else {
      if (p.pos(0) <= 0.5) eval.FP
      else eval.TN
    }
}
