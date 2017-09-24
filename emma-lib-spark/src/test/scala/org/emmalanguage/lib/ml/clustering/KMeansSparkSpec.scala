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
package lib.ml.clustering

import api._
import lib.linalg._
import lib.ml._

class KMeansSparkSpec extends KMeansSpec with SparkAware {

  override val runs = 2
  override val iterations = 2
  override val overlap = .30

  override def run(k: Int, input: String): Set[kMeans.Solution[Long]] =
    withDefaultSparkSession(implicit spark => emma.onSpark {
      // read the input
      val points = for (line <- DataBag.readText(input)) yield {
        val record = line.split("\t")
        DPoint(record.head.toLong, dense(record.tail.map(_.toDouble)))
      }
      // do the clustering
      val result = kMeans(2, k, runs, iterations)(points)
      // return the solution as a local set
      result.collect().toSet[kMeans.Solution[Long]]
    })
}
