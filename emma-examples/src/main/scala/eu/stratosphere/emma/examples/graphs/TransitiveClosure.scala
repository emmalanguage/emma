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

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine

import net.sourceforge.argparse4j.inf.{Subparser, Namespace}

class TransitiveClosure(input: String, output: String, rt: Engine) extends Algorithm(rt) {
  import TransitiveClosure._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get(TransitiveClosure.Command.input),
    ns.get(TransitiveClosure.Command.output),
    rt)

  def run() = algorithm.run(rt)

  val algorithm = emma.parallelize {
    // read in a directed graph
    var edges = read(input, new CSVInputFormat[Edge[V]]).distinct()

    var sizeO = 0L         // old size
    var sizeN = edges.size // new size

    while (sizeN - sizeO > 0) {
      val closure = for {
        e1 <- edges
        e2 <- edges
        if e1.dst == e2.src
      } yield Edge(e1.src, e2.dst)
      edges = (edges plus closure).distinct()
      sizeO = sizeN
      sizeN = edges.size
      println(s"Added ${sizeN - sizeO} edges")
    }

    write(output, new CSVOutputFormat[Edge[V]]) { edges }
    edges
  }
}

object TransitiveClosure {
  type V = Long

  class Command extends Algorithm.Command[TransitiveClosure] {
    import Command._
    val name = "trans-closure"
    val description = "Compute the transitive closure of a directed graph."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(input)
        .`type`(classOf[String])
        .dest(input)
        .metavar("INPUT")
        .help("graph edges")

      parser.addArgument(output)
        .`type`(classOf[String])
        .dest(output)
        .metavar("OUTPUT")
        .help("transitive closure")
    }
  }

  object Command {
    val input = "input"
    val output = "output"
  }
}
