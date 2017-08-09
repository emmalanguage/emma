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
package lib.ml.optimization.loss

import lib.linalg._
import lib.ml.LDPoint

// FIXME: does not compile with `@emma.lib`
object crossentropy extends Loss {

  /**
   * Compute the cross entropy loss function:
   *
   * @param w The weights that are used to evaluate the loss.
   * @param x The instance that is evaluated.
   * @return The loss as measured by the least squares solution.
   */
  def apply[ID](w: DVector, x: LDPoint[ID, Double]): Double = {
    val y = logistic(w dot x.pos)
    x.label * Math.log(y) + (1.0 - x.label) * Math.log(1.0 - y)
  }

  /**
   * Compute the gradient of the cross entropy loss function.
   *
   * @param w The weights for which the gradient is computed.
   * @param x The instance for which the gradient is computed.
   * @return The computed gradient.
   */
  def gradient[ID](w: DVector, x: LDPoint[ID, Double]): DVector = {
    val y = logistic(w dot x.pos)
    x.pos * (y - x.label)
  }

  private def logistic(x: Double): Double = {
    val r = 1.0 / (1.0 + Math.exp(-1 * x))
    if (r <= 0.0) 0.0001 // clip left
    else if (r >= 1.0) 0.9999 // clip right
    else r // don't clip
  }

}
