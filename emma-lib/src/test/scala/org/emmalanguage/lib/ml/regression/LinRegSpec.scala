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
import lib.ml.optimization._
import lib.ml.testutil._

import scala.util.Random

class LinRegSpec extends lib.BaseLibSpec {
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
  val weights = dense(Array(0.0, 0.0))

  // initial seed for the test problems
  val seed = 0L

  val instances = for (i <- 1 to N) yield {
    val x = r.nextDouble() * 10.0 - 5.0
    LDPoint(i, dense(Array(1.0, x)), -7.0 + 3.0 * x)
  }

  "logreg.train" should "fit a linear function correctly" in {
    val exp = solve(instances)
    val act = run(instances)(weights, seed)

    norm(dense(exp.zip(act.weights.values).map(v => v._1 - v._2)), 2) should be < expRadius
  }

  def run(instances: Seq[LDPoint[Int, Double]])(weights: DVector, seed: Long): LinearModel = {
    val insts = DataBag(instances.map(x => x.copy(pos = dense(x.pos.values.drop(1)))))

    val solve = solver.sgd[Int](
      learningRate = learningRate,
      iterations = iterations,
      miniBatchSize = miniBatchSize,
      lambda = lambda,
      seed = seed
    )(
      error.rmse,
      regularization.l2
    )(
      weights
    )(_)

    linreg.train(insts, solve)
  }
}
