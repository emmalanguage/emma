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
package lib.ml.regression

import api._
import lib.linalg._
import lib.ml._
import lib.ml.optimization.error.Error

@emma.lib
object linreg {
  type Instance[ID] = LDPoint[ID, Double]
  type Solver[ID] = DataBag[Instance[ID]] => LinearModel

  def train[ID: Meta](instances: DataBag[Instance[ID]], solve: Solver[ID]): LinearModel =
    solve(prependBias(instances))

  def predict[ID: Meta](
    weights: DVector, err: Error
  )(
    instances: DataBag[Instance[ID]]
  ): Double = err(weights, prependBias(instances))

  def prependBias[ID: Meta](instances: DataBag[Instance[ID]]): DataBag[Instance[ID]] =
    instances.map(x => x.copy(pos = dense(1.0 +: x.pos.values)))
}
