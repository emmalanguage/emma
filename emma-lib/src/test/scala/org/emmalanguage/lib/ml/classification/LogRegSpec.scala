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

import scala.util.Random

class LogRegSpec extends lib.BaseLibSpec {
  val r = new Random(53142165L)
  val N = 500

  // hyperparameters for the SGD solver
  val learningRate = 0.5
  val iterations = 500
  val miniBatchSize = 100
  val lambda = 0.25

  // threshold for the mispredicted elements
  val maxMispredicted = 5

  // initial weights for the test problems
  val weights = dense(Array(0.0, 0.0))

  // initial seed for the test problems
  val seed = 0L

  val instances = for (i <- 1 to N) yield {
    val x = r.nextDouble() * 100.0 - 50.0
    LDPoint(i, dense(Array(1.0, x)), if (x > 0.0) +1.0 else -1.0)
  }

  "logreg.train" should "seperate two classes correctly" in {
    val model = run(instances)(weights, seed)

    val mispredicted = instances.map(x => {
      val act = if ((model.weights dot x.pos) > 0.0) 1.0 else -1.0
      val exp = x.label
      if (exp == act) 0 else 1
    }).sum

    mispredicted should be < maxMispredicted
  }

  def run(instances: Seq[LDPoint[Int, Double]])(weights: DVector, seed: Long): LinearModel = {
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
  }
}
