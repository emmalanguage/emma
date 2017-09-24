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
package compiler.benchmark

import api._
import compiler.RuntimeCompiler
import examples.graphs._
import examples.graphs.model._
import examples.imdb._
import examples.text._

import org.scalameter._
import shapeless.cachedImplicit

/** Common methods and mixins for all compier benchmarks. */
trait BaseCompilerBench extends Bench.OnlineRegressionReport {

  val compiler = new RuntimeCompiler()
  import compiler._
  import u.reify

  val csv   = CSV()
  val input = "/tmp/dummy.csv"

  // ToolBox.typecheck can't handle type class derivation.
  implicit val edgeCSVConverter:  CSVConverter[Edge[Long]]      = cachedImplicit
  //implicit val lvecCSVConverter:  CSVConverter[LVector[String]] = cachedImplicit
  //implicit val pointCSVConverter: CSVConverter[Point[Long]]     = cachedImplicit
  //implicit def breezeVecCSVConverter[A: ClassTag: CSVColumn]: CSVConverter[Vector[A]] =
  //  CSVConverter.iso[Array[A], Vector[A]](Iso.make(DenseVector.apply, _.toArray), implicitly)

  // ---------------------------------------------------------------------------
  // Transformation pipelines
  // ---------------------------------------------------------------------------

  val expandPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true)(
      Lib.expand
    ).compose(_.tree)

  // ---------------------------------------------------------------------------
  // Benchmarking examples
  // ---------------------------------------------------------------------------

  val connectedComponents = reify {
    val edges = DataBag.readCSV[Edge[Long]](input, csv)
    ConnectedComponents(edges)
  }

  val enumerateTriangles = reify {
    val edges = DataBag.readCSV[Edge[Long]](input, csv)
    EnumerateTriangles(edges)
  }

  // FIXME: migrate to the `emma-lib` version
  /*val transitiveClosure = reify {
    val edges = DataBag.readCSV[graphs.model.Edge[Long]](input, csv)
    TransitiveClosure(edges)
  }*/

  val directorsMuses = reify {
    DirectorsMuses(input, csv)("John Doe")
  }

  // FIXME: Doesn't work
  //  val graphPreprocessing = reify {
  //    GraphPreprocessing(input, csv) {
  //      _.count(c => c.director == c.actor)
  //    }
  //  }

  // FIXME: migrate to the `emma-lib` version
  /*val naiveBayes = reify {
    val modelType = NaiveBayes.ModelType.Bernoulli
    val data = DataBag.readCSV[LVector[String]](input, csv)
    NaiveBayes(1.0, modelType)(data)
  }*/

  // FIXME: migrate to the `emma-lib` version
  /*val kMeans = reify {
    val points = DataBag.readCSV[Point[Long]](input, csv)
    KMeans[Long](2, 8, 1e-3, 10)(points)
  }*/

  val wordCount = reify {
    val documents = DataBag.readText(input)
    WordCount(documents)
  }

  val examples = Seq(
    connectedComponents,
    enumerateTriangles,
    //transitiveClosure,
    directorsMuses,
    //graphPreprocessing,
    //naiveBayes,
    //kMeans,
    wordCount
  ).map(expandPipeline)
}
