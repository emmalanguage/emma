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
import org.scalatest.prop.PropertyChecks

import scala.io.Source

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class LabelRankTest extends FlatSpec with PropertyChecks with Matchers with BeforeAndAfter {
  // default parameters
  val dir             = "/graphs/label-rank"
  val path            = tempPath(dir)
  val maxOscillations = 5
  val inflation       = 2
  val cutoff          = 0.1
  val similarity      = 0.3
  val epsilon         = 0.15

  before {
    new File(path).mkdirs()
    materializeResource(s"$dir/edges.tsv")
  }

  after {
    deleteRecursive(new File(path))
  }

  lazy val vertices = Source.fromFile(s"$path/edges.tsv").getLines().flatMap {
    line => line split "\t" map { _.toLong }
  }.toStream.distinct.size

  lazy val edges = Source.fromFile(s"$path/edges.tsv").getLines().size

  "LabelRank" should "approximately detect communities with known average size" in {
    vertices / testLabelRank().toDouble should (be > 3.0 and be < 6.0)
  }

  def testLabelRank(
      maxOscillations: Int    = maxOscillations,
      inflation:       Int    = inflation,
      cutoff:          Double = cutoff,
      similarity:      Double = similarity) = withRuntime() { rt =>
    val labelRank = new LabelRank(
      s"$path/edges.tsv", s"$path/labels",
      maxOscillations, inflation, cutoff, similarity, rt
    ).algorithm

    val labels = labelRank.run(rt).fetch().map { _.label }.distinct.size
    println(s"$labels communities of $vertices vertices and $edges edges")
    labels
  }
}
