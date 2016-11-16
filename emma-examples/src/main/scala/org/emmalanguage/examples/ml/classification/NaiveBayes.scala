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
package examples.ml.classification

import api.Meta.Projections._
import api._
import examples.ml.model._

import breeze.linalg._

@emma.lib
object NaiveBayes {

  type ModelType = ModelType.Value
  type Model[L] = (L, Double, Vector[Double])

  def apply[L: Meta](
    lambda: Double, modelType: ModelType // hyper-parameters
  )(
    data: DataBag[LVector[L]] // data-parameters
  ): DataBag[Model[L]] = {
    val dimensions = data.map(_.vector.length).distinct.fetch()
    assert(dimensions.size == 1, "Multiple dimensions in input data. All vectors should have the same length.")
    val N = dimensions.head

    val aggregated = for (Group(label, values) <- data.groupBy(_.label)) yield {
      val lCnt = values.size
      val lSum = values.fold(Vector.zeros[Double](N))(_.vector, _ + _)
      (label, lCnt, lSum)
    }

    val numPoints = data.size
    val numLabels = aggregated.size
    val priorDenom = math.log(numPoints + numLabels * lambda)

    val model = for ((label, lCnt, lSum) <- aggregated) yield {
      val prior = math.log(lCnt + lambda) - priorDenom

      val evidenceDenom =
        if (modelType == ModelType.Multinomial) math.log(sum(lSum) + lambda * N)
        else /* bernoulli */ math.log(lCnt + 2.0 * lambda)

      val evidence = for {
        x <- lSum
      } yield math.log(x + lambda) - evidenceDenom

      (label, prior, evidence)
    }

    model
  }

  object ModelType extends Enumeration {
    val Multinomial = Value("multinomial")
    val Bernoulli = Value("bernoulli")
  }

}
