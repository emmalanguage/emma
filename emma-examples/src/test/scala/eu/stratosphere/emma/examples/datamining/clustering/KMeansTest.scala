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

import java.io.File
import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import scala.io.Source

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class KMeansTest extends FlatSpec with Matchers with BeforeAndAfter {
  // default parameters
  val dir = "/clustering/kmeans"
  val path = tempPath(dir)
  val epsilon = 1e-3
  val iterations = 5
  val delimiter = "\t"
  val threshold = .75

  before {
    new File(path).mkdirs()
    materializeResource(s"$dir/points.tsv")
    materializeResource(s"$dir/clusters.tsv")
  }

  after {
    deleteRecursive(new File(path))
  }

  "KMeans" should "cluster points around the corners of a hypercube" ignore withRuntime() { rt =>
    val expected = clusters(Source
      .fromFile(s"$path/clusters.tsv")
      .getLines().map { line =>
        val record = line.split(delimiter).map { _.toLong }
        record.head -> record(1)
      }.toStream)

    val solution = new KMeans(expected.size, epsilon, iterations,
      s"$path/points.tsv", s"$path/solutions.tsv", rt).algorithm.run(rt).fetch()

    val actual = clusters(solution)
    val clusteringCoefficient = (for {
      act <- actual
      exp <- expected
      if (act & exp).size / exp.size.toDouble >= threshold
    } yield ()).size / expected.size.toDouble
    clusteringCoefficient should be >= threshold
  }

  def clusters(centers: Seq[(Long, Long)]): Iterable[Set[Long]] =
    centers.toSet[(Long, Long)].groupBy { _._2 }.values.map { _.map { _._1 } }
}
