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

import examples.graphs.model._
import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File

trait BaseConnectedComponentsIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")

  before {
    new File(codegenDir).mkdirs()
    addToClasspath(new File(codegenDir))
  }

  after {
    deleteRecursive(new File(codegenDir))
  }

  it should "find all connected sub-graphs" in {
    val act = connectedComponents(Seq(
      Edge(1, 2), Edge(1, 5), Edge(1, 7),
      Edge(2, 1), Edge(2, 5), Edge(2, 7),
      Edge(3, 3),
      Edge(4, 6),
      Edge(5, 1), Edge(5, 2),
      Edge(6, 4),
      Edge(7, 1),
      Edge(7, 2)
    ))

    val exp = Seq(
      LVertex(1, 7),
      LVertex(2, 7),
      LVertex(3, 3),
      LVertex(4, 6),
      LVertex(5, 7),
      LVertex(6, 6),
      LVertex(7, 7)
    )

    act should contain theSameElementsAs exp
  }

  def connectedComponents(edges: Seq[Edge[Int]]): Seq[LVertex[Int, Int]]
}
