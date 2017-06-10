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

import api._
import examples.graphs.model.Edge
import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import resource._

import java.io.File
import java.io.PrintWriter

trait BaseTransitiveClosureIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/graphs/trans-closure"
  val path = tempPath(dir)

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    generateInput(s"$path/edges.tsv")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  it should "compute the transitive closure of a directed graph" in {
    val act = transitiveClosure(s"$path/edges.tsv", CSV())
    val exp = expectedClosure()

    act should contain theSameElementsAs exp
  }

  def transitiveClosure(input: String, csv: CSV): Set[Edge[Long]]

  lazy val paths = {
    val S = 3415434314L
    val P = 5

    val ws = shuffle(P)(util.RanHash(S, 0)).map(_.toLong)
    val xs = shuffle(P)(util.RanHash(S, 1)).map(_.toLong + P)
    val ys = shuffle(P)(util.RanHash(S, 2)).map(_.toLong + P * 2)
    val zs = shuffle(P)(util.RanHash(S, 3)).map(_.toLong + P * 3)

    ws zip xs zip ys zip zs
  }

  private def generateInput(path: String): Unit = {
    val edges = {
      for {
        (((w, x), y), z) <- paths
        e <- Seq(Edge(w, x), Edge(x, y), Edge(y, z))
      } yield e
    }.distinct

    for (pw <- managed(new PrintWriter(new File(path))))
      yield for (e <- edges.sortBy(_.src)) pw.write(s"${e.src}\t${e.dst}\n")
  }.acquireAndGet(_ => ())

  private def expectedClosure(): Set[Edge[Long]] = {
    for {
      (((w, x), y), z) <- paths
      e <- Seq(Edge(w, x), Edge(x, y), Edge(y, z), Edge(w, y), Edge(x, z), Edge(w, z))
    } yield e
  }.toSet

  private def shuffle(n: Int)(r: util.RanHash): Array[Int] = {
    val xs = (1 to n).toArray
    for {
      i <- 0 to (n - 2)
    } {
      val j = r.nextInt(i + 1)
      val t = xs(i)
      xs(i) = xs(j)
      xs(j) = t
    }
    xs
  }
}
