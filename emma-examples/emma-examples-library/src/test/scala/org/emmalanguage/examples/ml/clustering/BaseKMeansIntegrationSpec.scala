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
import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.io.Source

import java.io.File

trait BaseKMeansIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/ml/clustering/kmeans"
  val path = tempPath(dir)
  val epsilon = 1e-3
  val iterations = 10
  val delimiter = "\t"
  val overlap = .75

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    materializeResource(s"$dir/points.tsv")
    materializeResource(s"$dir/clusters.tsv")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  it should "cluster points around the corners of a hypercube" in {
    val exp =
      clusters(for {
        line <- Source.fromFile(s"$path/clusters.tsv").getLines().toSet[String]
      } yield {
        val Seq(point, cluster) = line.split(delimiter).map(_.toLong).toSeq
        point -> cluster
      })

    val act =
      clusters(for {
        s <- kMeans(exp.size, epsilon, iterations, s"$path/points.tsv")
      } yield (s.id, s.label.id))

    val correctClusters = for {
      act <- act
      exp <- exp
      if (act & exp).size / exp.size.toDouble >= overlap
    } yield ()

    correctClusters.size.toDouble should be >= (overlap * exp.size)
  }

  def clusters(associations: Set[(Long, Long)]): Iterable[Set[Long]] =
    for ((_, values) <- associations.groupBy { case (p, c) => c })
      yield values.map { case (p, c) => p }

  def kMeans(size: Int, epsilon: Double, iterations: Int, input: String): Set[Solution[Long]]
}
