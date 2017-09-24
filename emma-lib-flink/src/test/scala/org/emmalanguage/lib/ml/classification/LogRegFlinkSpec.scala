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
import lib.ml.optimization._

class LogRegFlinkSpec extends LogRegSpec with FlinkAware {

  // hyperparameters for the SGD solver
  override val learningRate = 0.001
  override val iterations = 5
  override val miniBatchSize = 100
  override val lambda = 0.25

  // threshold for the mispredicted elements
  override val maxMispredicted = 5

  // initial weights for the test problems
  override val weights = dense(Array(0.0, 0.0))

  // initial seed for the test problems
  override val seed = -8827055269646172160L

  override def run(instances: Seq[LDPoint[Int, Double]])(weights: DVector, seed: Long): LinearModel =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      val insts = DataBag(instances.map(x => x.copy(pos = dense(x.pos.values.drop(1)))))

      val solve = solver.sgd[Int](
        learningRate = learningRate,
        iterations = iterations,
        miniBatchSize = miniBatchSize,
        lambda = lambda
      )(
        error.crossentropy,
        regularization.l2
      )(
        weights
      )(_)

      logreg.train(insts, solve)
    })
}
