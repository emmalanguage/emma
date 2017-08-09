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
package lib.ml.optimization.error

import api._
import lib.linalg._
import lib.ml.LDPoint
import lib.ml.optimization.loss.squared
import lib.stats.stat

/**
 * Root-Mean-Square Error (RMSE).
 *
 * The root ensures that RMSE is measures on the same scale (and in the same units) as the target variable.
 * This error function is usually used for evaluation and therefore we don't provide the gradient. For trainig
 * the regular MSE works just as well.
 *
 * loss:      E(w) = sqrt { MSE }
 * gradient: dE(w) = ???
 *
 */
@emma.lib
object rmse extends Error {

  def apply[ID](
    weights: DVector, instances: DataBag[LDPoint[ID, Double]]
  ): Double = Math.sqrt(1.0 / (2.0 * instances.size) * instances.map(squared(weights, _)).sum)

  def gradient[ID](
    weights: DVector, instances: DataBag[LDPoint[ID, Double]]
  ): DVector = stat.sum(weights.size)(instances.map(squared.gradient(weights, _))) / instances.size
}
