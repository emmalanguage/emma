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

@emma.lib
object eval {
  type Hypothesis = DVector => Boolean
  type TestBag[ID] = DataBag[LDPoint[ID, Boolean]]

  val TP = 0
  val FP = 1
  val FN = 2
  val TN = 3

  def apply[ID: Meta](h: Hypothesis)(xs: TestBag[ID]): DataBag[LDPoint[ID, Int]] =
    for (x <- xs) yield {
      val quadrant = (if (h(x.pos)) 0 else 2) | (if (x.label) 0 else 1)
      LDPoint(x.id, x.pos, quadrant)
    }

  def precision[ID: Meta](h: Hypothesis)(xs: TestBag[ID]): Double = {
    val es = eval(h)(xs)
    val tp = es.count(_.label == TP).toDouble
    val fp = es.count(_.label == FP).toDouble
    tp / (tp + fp)
  }

  def recall[ID: Meta](h: Hypothesis)(xs: TestBag[ID]): Double = {
    val es = eval(h)(xs)
    val tp = es.count(_.label == TP).toDouble
    val fn = es.count(_.label == FN).toDouble
    tp / (tp + fn)
  }

  def f1score[ID: Meta](h: Hypothesis)(xs: TestBag[ID]): Double = {
    val p = precision(h)(xs)
    val r = recall(h)(xs)
    2.0 * (p * r) / (p + r)
  }
}
