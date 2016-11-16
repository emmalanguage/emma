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
package eu.stratosphere.emma.examples.datamining.classification

import java.io.File

import breeze.linalg.DenseVector
import eu.stratosphere.emma.examples.datamining.classification.NaiveBayes.Bernoulli
import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.io.Source

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class NaiveBayesTest extends FlatSpec with Matchers with BeforeAndAfter {
  val dir = "/classification/naivebayes"
  val path = tempPath(dir)

  before {
    new File(path).mkdirs()
    materializeResource(s"$dir/vote.csv")
    materializeResource(s"$dir/model.txt")
  }

  after {
    deleteRecursive(new File(path))
  }

  "NaiveBayes" should "create the correct model on Bernoulli house-votes-84 data set" ignore withRuntime() { rt =>
    val expectedModel = Source
      .fromFile(s"$path/model.txt")
      .getLines().map { line =>
      val values = line.split(",").map(_.toDouble).toList
      (values.head, values(1), new DenseVector[Double](values.slice(2, values.size).toArray))
    }.toSeq

    val solution = new NaiveBayes(s"$path/vote.csv", 1.0, Bernoulli, rt).algorithm.run(rt).fetch()

    solution should contain theSameElementsAs expectedModel
  }
}
