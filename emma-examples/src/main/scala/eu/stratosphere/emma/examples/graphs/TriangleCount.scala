package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

class TriangleCount(input: String, output: String, rt: Engine)
    extends Algorithm(rt) {
  import eu.stratosphere.emma.examples.graphs.TriangleCount._
  import Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns get TriangleCount.Command.keyInput, "", rt)

  def run() = algorithm run rt

  val algorithm = emma.parallelize {
    // read an undirected graph
    val incoming = read(input, new CSVInputFormat[Schema.Edge])
    val outgoing = for (Edge(src, dst) <- incoming) yield Edge(dst, src)
    val edges    = (incoming plus outgoing).distinct()
    // generate all paths of length 2
    val paths2 = for {
      e1 <- edges
      e2 <- edges
      if e1.dst == e2.src
      if e1.src <  e2.dst
    } yield Edge(e1.src, e2.dst)
    // count all triangles (3 times each)
    val triangles = (for {
      e <- edges
      p <- paths2
      if e == p
    } yield p).size / 3
    // print the result to the console
    println(s"The number of triangles in the graph is $triangles")
    triangles
  }
}

object TriangleCount {
  class Command extends Algorithm.Command[TriangleCount] {
    import Command._

    override def name = "tc"

    override def description =
      "Count the number of triangle cliques in the graph."

    override def setup(parser: Subparser) = {
      super.setup(parser)
      parser.addArgument(keyInput)
        .`type`(classOf[String])
        .dest(keyInput)
        .metavar("INPUT")
        .help("graph edges")
    }
  }

  object Command {
    val keyInput = "input"
  }

  object Schema {
    type Vid = Long

    case class Edge(@id src: Vid, @id dst: Vid)
      extends Identity[(Vid, Vid)] { def identity = src -> dst }
  }
}
