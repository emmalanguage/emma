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
package examples.ml.clustering

import KMeans.Solution
import api.Meta.Projections._
import api._
import examples.ml.model._

class FlinkKMeansIntegrationSpec extends BaseKMeansIntegrationSpec with FlinkAware {

  override def kMeans(k: Int, epsilon: Double, iterations: Int, input: String): Set[Solution[Long]] =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      // read the input
      val points = for (line <- DataBag.readText(input)) yield {
        val record = line.split("\t")
        Point(record.head.toLong, record.tail.map(_.toDouble))
      }
      // do the clustering
      val result = KMeans(2, k, epsilon, iterations)(points)
      // return the solution as a local set
      result.collect().toSet[Solution[Long]]
    })
}
