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

class NaiveBayesFlinkSpec extends NaiveBayesSpec with FlinkAware {
  override def run(input: String, lambda: Double, modelType: MType) = {
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      val data = for ((line, index) <- DataBag.readText(input).zipWithIndex()) yield {
        val record = line.split(",").map(_.toDouble)
        val label = record.head
        val dVector = dense(record.slice(1, record.length))
        LDPoint(index, dVector, label)
      }
      // classification
      val result = naiveBayes(16, lambda, modelType)(data)
      // collect the result locally
      result.collect().toSet[naiveBayes.Model[Double]]
    })
  }
}
