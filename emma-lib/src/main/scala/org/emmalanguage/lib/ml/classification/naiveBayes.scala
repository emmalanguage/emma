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
import lib.stats._

@emma.lib
object naiveBayes {

  type ModelType = ModelType.Value
  case class Model[L](label: L, pi: Double, theta: DVector) {
    def this(label: L, pi: Double, theta: Vector) = this(label, pi, theta match {
      case pos: DVector => pos
      case pos: SVector => pos.toDense
    })
  }

  def apply[ID: Meta, L: Meta](D: Int,
    lambda: Double, modelType: ModelType // hyper-parameters
  )(
    data: DataBag[LDPoint[ID, L]] // data-parameters
  ): DataBag[Model[L]] = {

    val aggregated = for (Group(label, values) <- data.groupBy(_.label)) yield {
      val lCnt = values.size
      val lSum = stat.sum(D)(values.map(_.pos))
      (label, lCnt, lSum)
    }

    val numPoints = data.size
    val numLabels = aggregated.size
    val priorDenom = math.log(numPoints + numLabels * lambda)

    val model = for ((label, lCnt, lSum) <- aggregated) yield {
      val prior = math.log(lCnt + lambda) - priorDenom

      val evidenceDenom =
        if (modelType == ModelType.Multinomial) math.log(sum(lSum) + lambda * D)
        else /* bernoulli */ math.log(lCnt + 2.0 * lambda)

      val evidence = dense(for {
        x <- lSum.values
      } yield math.log(x + lambda) - evidenceDenom)

      Model(label, prior, evidence)
    }

    model
  }

  object ModelType extends Enumeration {
    val Multinomial = Value("multinomial")
    val Bernoulli = Value("bernoulli")
  }

}
