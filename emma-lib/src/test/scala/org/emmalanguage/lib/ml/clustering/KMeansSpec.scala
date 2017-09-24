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
import test.util._

import scala.io.Source

class KMeansSpec extends lib.BaseLibSpec {

  val delimiter = "\t"

  val runs = 2
  val iterations = 2
  val overlap = .75

  val path = "/ml/clustering/kmeans"
  val temp = tempPath(path)

  override def tempPaths: Seq[String] = Seq(path)

  override def resources = for {
    file <- Seq("points.tsv", "clusters.tsv")
  } yield () => {
    materializeResource(s"$path/$file"): Unit
  }

  "kMeans" should "cluster points around the corners of a hypercube" in {
    val exp = clusters(for {
      line <- Source.fromFile(s"$temp/clusters.tsv").getLines().toSet[String]
    } yield {
      val Seq(pointID, clusterID) = line.split(delimiter).map(_.toLong).toSeq
      LDPoint(pointID, dense(Array(0.0)), DPoint(clusterID, dense(Array(0.0))))
    })

    val act = clusters(for {
      s <- run(exp.size, s"$temp/points.tsv")
    } yield s)

    val correctClusters = for {
      act <- act
      exp <- exp
      if (act & exp).size / exp.size.toDouble >= overlap
    } yield ()

    correctClusters.size shouldBe exp.size
  }

  def clusters(solution: Set[kMeans.Solution[Long]]): Iterable[Set[Long]] =
    solution.groupBy(_.label.id).mapValues(_.map(_.id).toSet).values

  def run(k: Int, input: String): Set[kMeans.Solution[Long]] = {
    // read the input
    val points = for (line <- DataBag.readText(input)) yield {
      val record = line.split("\t")
      DPoint(record.head.toLong, dense(record.tail.map(_.toDouble)))
    }
    // do the clustering
    val result = kMeans(2, k, runs, iterations)(points)
    // return the solution as a local set
    result.collect().toSet[kMeans.Solution[Long]]
  }
}
