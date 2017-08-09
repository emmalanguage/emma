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
package examples.graphs

import api._
import model.Edge

class SparkTriangleCountIntegrationSpec extends BaseTriangleCountIntegrationSpec with SparkAware {

  override def triangleCount(input: String, csv: CSV): Long =
    withDefaultSparkSession(implicit spark => emma.onSpark {
      // read a bag of directed edges
      // and convert it into an undirected bag without duplicates
      val incoming = DataBag.readCSV[Edge[Long]](input, csv)
      val outgoing = incoming.map(e => Edge(e.dst, e.src))
      val edges = (incoming union outgoing).distinct
      // compute all triangles
      val triangles = EnumerateTriangles(edges)
      // count and return the number of enumerated triangles
      triangles.size
    })
}
