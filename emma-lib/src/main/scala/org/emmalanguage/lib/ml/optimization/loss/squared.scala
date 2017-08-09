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

/**
 * Represents a loss function of the form
 *
 * E(w) = (wx - y)**2
 *
 * where
 * w: Weights
 * x: Instance features
 * y: Instance label
 */
object squared extends Loss {

  /**
   * Compute the squared loss [[Loss]] function.
   *
   * @param w The weights that are used to evaluate the loss.
   * @param x The instance that is evaluated.
   * @return The loss as measured by the least squares solution.
   */
  def apply[ID](w: DVector, x: LDPoint[ID, Double]): Double = {
    val residual = (w dot x.pos) - x.label
    residual * residual
  }

  /**
   * Compute the gradient of the squared loss [[Loss]] function.
   *
   * {{{
   * dE(w) = (wx - y)x
   * }}}
   *
   * @param w The weights for which the gradient is computed.
   * @param x The instance for which the gradient is computed.
   * @return The computed gradient.
   */
  def gradient[ID](w: DVector, x: LDPoint[ID, Double]): DVector = {
    val residual = (w dot x.pos) - x.label
    val gradient = x.pos * residual
    gradient
  }
}
