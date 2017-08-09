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
import lib.ml.testutil._

import scala.util.Random

class SGDSpec extends lib.BaseLibSpec {
  val r = new Random(53142165L)
  val N = 500

  // hyperparameters for the SGD solver
  val learningRate = 0.5
  val iterations = 500
  val miniBatchSize = 100
  val lambda = 0.25

  // threshold radius for the hypersphere of the expected result
  val expRadius = 1e+0

  // initial weights for the test problems
  val weights = Map(
    1 -> dense(Array(0.0, 0.0)),
    2 -> dense(Array(0.0, 0.0))
  )

  // initial seeds for the test problems
  val seeds = Map(
    1 -> 0L,
    2 -> 0L
  )

  val inst1 = for (i <- 1 to N) yield {
    val x = r.nextDouble() * 10.0 - 5.0
    LDPoint(i, dense(Array(1.0, x)), -7.0 + 3.0 * x)
  }

  val inst2 = for (i <- 1 to N) yield {
    val x = r.nextDouble() * 10.0 - 5.0
    LDPoint(i, dense(Array(1.0, x)), -1.0 + 5.0 * x)
  }

  "SGD solver" should "compute the correct weights for problem #1" in {
    val exp = solve(inst1)
    val act = run(inst1)(weights(1), seeds(1))

    norm(dense(exp.zip(act.weights.values).map(v => v._1 - v._2)), 2) should be < expRadius
  }

  it should "compute the correct weights for problem #2" in {
    val exp = solve(inst2)
    val act = run(inst2)(weights(2), seeds(2))

    norm(dense(exp.zip(act.weights.values).map(v => v._1 - v._2)), 2) should be < expRadius
  }

  def run(instances: Seq[LDPoint[Int, Double]])(weights: DVector, seed: Long): LinearModel =
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
}
