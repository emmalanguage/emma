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

import examples.graphs.model.Edge
import io.csv.CSV
import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.io.Source

import java.io.File

trait BaseTransitiveClosureSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val dir = "/graphs/trans-closure"
  val path = tempPath(dir)

  before {
    new File(path).mkdirs()
    materializeResource(s"$dir/edges.tsv")
  }

  after {
    deleteRecursive(new File(path))
  }

  "TransitiveClosure" should "compute the transitive closure of a directed graph" in {
    val graph = (for {
      line <- Source.fromFile(s"$path/edges.tsv").getLines
    } yield {
      val record = line.split('\t').map(_.toLong)
      Edge(record(0), record(1))
    }).toSet

    val closure = transitiveClosure(s"$path/edges.tsv", CSV())

    graph subsetOf closure should be(true)
  }

  def transitiveClosure(input: String, csv: CSV): Set[Edge[Long]]
}
