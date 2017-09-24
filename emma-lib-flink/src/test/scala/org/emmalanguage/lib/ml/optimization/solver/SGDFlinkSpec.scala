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
package lib.ml.optimization.solver

import api._
import lib.linalg._
import lib.ml._
import lib.ml.optimization._

class SGDFlinkSpec extends SGDSpec with FlinkAware {

  // hyperparameters for the SGD solver
  override val learningRate = 0.001
  override val iterations = 5
  override val miniBatchSize = 100
  override val lambda = 0.25

  // threshold radius for the hypersphere of the expected result
  override val expRadius = 1e+3

  // initial weights for the test problems
  override val weights = Map(
    1 -> dense(Array(-6.227952008238842, 3.0752773753240046)),
    2 -> dense(Array(-1.7750851821560918, 4.91356783246175))
  )

  // initial seeds for the test problems
  override val seeds = Map(
    1 -> -8827055269646172160L,
    2 -> -8827055269646172160L
  )

  override def run(instances: Seq[LDPoint[Int, Double]])(weights: DVector, seed: Long): LinearModel =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      sgd(
        learningRate = learningRate,
        iterations = iterations,
        miniBatchSize = miniBatchSize,
        lambda = lambda,
        seed = seed
      )(
        error.rmse,
        regularization.l2
      )(weights)(DataBag(instances))
    })
}
