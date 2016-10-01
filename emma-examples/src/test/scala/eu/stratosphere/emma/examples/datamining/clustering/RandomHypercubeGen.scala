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
package eu.stratosphere.emma.examples.datamining.clustering

import java.io.{File, PrintWriter}
import eu.stratosphere.emma.testutil._
import scala.util.Random

/**
 * Generate test data for the Belief Propagation algorithm from some individual
 * and collocated word frequency data.
 *
 * @see http://www.wordfrequency.info/
 * @see http://www.collocates.info/
 */
object RandomHypercubeGen extends App {

  val dimensions = 2 // input dimensions
  val scale = 100 // length of the hypercube edge
  val spread = math.sqrt(scale)
  val cardinality = 1000 // number of generated points

  val seed = 23454638945312l
  val rand = new Random(seed)

  val dir = "/clustering/hypercube"
  val path = tempPath(dir)
  new File(path).mkdirs()

  val delimiter = "\t"
  val pointWriter = new PrintWriter(s"$path/points.tsv")
  val clusterWriter = new PrintWriter(s"$path/clusters.tsv")

  val currentPoint = Array.fill(dimensions)(0.0)
  val currentCenter = Array.fill(dimensions)(0)

  val clusters = (for (_ <- 1 to cardinality) yield {
    for (d <- 0 until dimensions) {
      currentCenter(d) = if (rand.nextBoolean()) 0 else scale
      currentPoint(d) = currentCenter(d) + (2 * rand.nextDouble() - 1) * spread
    }

    currentPoint.toVector -> currentCenter.toVector
  }).zipWithIndex.groupBy { case ((_, c), _) => c }
    .values.map { _.map { case ((p, _), i) => (p, i) } }
    .toVector.zipWithIndex

  try for {
    (pts, i) <- clusters
    (p, j) <- pts
  } {
    pointWriter.println(j +: p mkString delimiter)
    clusterWriter.println(s"$j\t$i")
  } finally {
    pointWriter.close()
    clusterWriter.close()
  }
}
