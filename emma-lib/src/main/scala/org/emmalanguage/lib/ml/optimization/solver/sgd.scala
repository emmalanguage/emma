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
import util.RanHash

import scala.collection.mutable.ArrayBuffer

@emma.lib
object sgd {

  def apply[ID: Meta](
    learningRate   : Double,
    iterations     : Int,
    miniBatchSize  : Int,
    lambda         : Double = 1.0,
    maxGrad        : Double = 100000.0,
    seed           : Long = 345345134231L
  )(
    err            : error.Error,
    reg            : regularization.Regularization = regularization.noRegularization
  )(
    initialWeights : DVector
  )(
    instances      : DataBag[LDPoint[ID, Double]]
  ): LinearModel = {

    // initialize weights
    var weights = initialWeights

    // initialize the loss history
    val lossHistory = new ArrayBuffer[Double](iterations)

    // start the actual solving iterations
    for (iter <- 1 to iterations) {
      // sample a subset of the data
      val batch = DataBag(instances.sample(miniBatchSize, RanHash(seed, iter - 1).seed))

      // sum the partial losses and gradients
      val loss = err(weights, batch) + lambda * reg(weights)
      var grad = err.gradient(weights, batch) + lambda * reg.gradient(weights)

      // protect against exploding gradients
      val gnrm = norm(grad, 2)
      if (gnrm > maxGrad) {
        grad = grad * maxGrad / gnrm
      }

      // compute learning rate for this iteration
      val lr = learningRate / math.sqrt(iter)

      // perform weight update
      // against the direction of the gradient and proportional to size of lr
      val newWeights = weights - (grad * lr)

      // println(s"iter = $iter: loss = $loss, lr = $lr, seed = ${RanHash(seed, iter - 1).seed} weights = $weights")

      // append loss for this iteration
      lossHistory.append(loss)

      // update weights and iteration count
      weights = newWeights
    }

    LinearModel(weights, lossHistory.toArray)
  }
}
