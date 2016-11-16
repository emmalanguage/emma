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
package eu.stratosphere.emma.examples.graphs

import java.io.File

import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.io.Source

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class TransitiveClosureTest extends FlatSpec with Matchers with BeforeAndAfter {
  // default parameters
  val dir = "/graphs/trans-closure"
  val path = tempPath(dir)

  before {
    new File(path).mkdirs()
    materializeResource(s"$dir/edges.tsv")
  }

  after {
    deleteRecursive(new File(path))
  }

  "TransitiveClosure" should "compute the transitive closure of a directed graph" ignore
    withRuntime() { rt =>
      val graph = Source.fromFile(s"$path/edges.tsv").getLines.map { line =>
        val record = line.split('\t').map(_.toLong)
        Edge(record(0), record(1))
      }.toSet

      val closure = new TransitiveClosure(s"$path/edges.tsv", s"$path/trans-closure", rt)
        .algorithm.run(rt).fetch().toSet

      graph subsetOf closure should be (true)
    }
}
