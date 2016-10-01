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

import java.io.{PrintWriter, File}

import eu.stratosphere.emma.testutil._

import scala.io.Source

/**
 * Generate test data for the Belief Propagation algorithm from some individual
 * and collocated word frequency data.
 *
 * @see http://www.wordfrequency.info/
 * @see http://www.collocates.info/
 */
object BeliefPropagationGen extends App {

  val total = 4.8e8 // total number of word in the corpus
  val known = 0.33  // number of observed words
  val dir   = "/graphs/belief-prop"
  val path  = tempPath(dir)

  new File(path).mkdirs()
  materializeResource(s"$dir/lemmas.txt")
  materializeResource(s"$dir/collocates.txt")

  val varWriter  = new PrintWriter(s"$path/variables")
  val potWriter  = new PrintWriter(s"$path/potential")

  val lemmas = (for {
    line <- Source.fromFile(s"$path/lemmas.txt").getLines()
  } yield {
      val record = line.split("\t")
      record(1) -> record(3).toDouble
    }).toMap

  val observed = lemmas.keySet.take((known * lemmas.size).toInt)

  val collocates = for {
    line <- Source.fromFile(s"$path/collocates.txt").getLines()
  } yield {
      val record = line.split("\t")
      (record(1), record(3), record(6).toDouble)
    }

  for {
    (w1, w2, freq) <- collocates
    if lemmas.contains(w1)
    if lemmas.contains(w2)
  } {
    val ff = freq
    val f1 = lemmas(w1) - freq
    val f2 = lemmas(w2) - freq

    potWriter.println(Seq(w1, w2, 1, 1, ff).mkString("\t"))
    potWriter.println(Seq(w1, w2, 1, 0, f1).mkString("\t"))
    potWriter.println(Seq(w1, w2, 0, 1, f2).mkString("\t"))

    if (observed(w1)) {
      varWriter.println(Seq(w1, 1, 1.0).mkString("\t"))
      varWriter.println(Seq(w1, 0, 0.0).mkString("\t"))
    } else {
      val p1 = f1 / total
      val n1 = 1 - p1
      varWriter.println(Seq(w1, 1, p1).mkString("\t"))
      varWriter.println(Seq(w1, 0, n1).mkString("\t"))
    }

    if (observed(w2)) {
      varWriter.println(Seq(w2, 1, 1.0).mkString("\t"))
      varWriter.println(Seq(w2, 0, 0.0).mkString("\t"))
    } else {
      val p2 = f2 / total
      val n2 = 1 - p2
      varWriter.println(Seq(w2, 1, p2).mkString("\t"))
      varWriter.println(Seq(w2, 0, n2).mkString("\t"))
    }
  }

  varWriter.close()
  potWriter.close()
}