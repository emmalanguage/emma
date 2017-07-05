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
package examples.ml.classification

import test.util._

import breeze.linalg.{Vector => Vec}
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.io.Source

import java.io.File

trait BaseNaiveBayesIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  type MType = NaiveBayes.ModelType
  type Model = NaiveBayes.Model[Double]

  val codegenDir = tempPath("codegen")
  val dir = "/ml.classification/naivebayes"
  val path = tempPath(dir)

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    materializeResource(s"$dir/vote.csv")
    materializeResource(s"$dir/model.txt")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  it should "create the correct model on Bernoulli house-votes-84 data set" in {
    val expectedModel = for {
      line <- Source.fromFile(s"$path/model.txt").getLines().toSet[String]
    } yield {
      val values = line.split(",").map(_.toDouble)
      (values(0), values(1), Vec(values.slice(2, values.length)))
    }

    val solution = naiveBayes(s"$path/vote.csv", 1.0, NaiveBayes.ModelType.Bernoulli)

    solution should contain theSameElementsAs expectedModel
  }

  def naiveBayes(input: String, lambda: Double, modelType: MType): Set[Model]
}
