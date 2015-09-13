package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import scala.annotation.tailrec

class GraphColoring(input: String, output: String, rt: Engine)
    extends Algorithm(rt) {
  import eu.stratosphere.emma.examples.graphs.GraphColoring._
  import Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns get GraphColoring.Command.keyInput,
    ns get GraphColoring.Command.keyOutput,
    rt)

  def run() = {
    val algorithm = /* emma.parallelize */ {
      // read an undirected graph and direct it from smaller to larger IDs
      val incoming   = read(input, new CSVInputFormat[Schema.Edge])
      val outgoing   = for (Edge(src, dst) <- incoming) yield Edge(dst, src)
      val undirected = (incoming plus outgoing).distinct()
      val directed   = for (e <- undirected; if e.src < e.dst) yield e

      // collect all vertices and initialize them with the minimal color
      var colored  = (for (Edge(src, _) <- undirected)
        yield ColoredVertex(src)).distinct()

      val vertices = stateful[Vertex, Vid] {
        for (ColoredVertex(id, _) <- colored) yield Vertex(id)
      }
      
      // initialize all messages with the minimal color
      var messages = for (Edge(src, dst) <- directed)
        yield Message(src, dst)

      // resolve conflicting colors until convergence
      while (messages.nonEmpty) {
        messages = vertices.updateWithMany(messages)(
          _.dst, (v, ms) => { // if the color is taken choose a new one
            if (ms exists { _.col == v.col }) {
              val colors = for {
                e <- directed
                if e.dst == v.id
                u <- colored
                if e.src == u.id
              } yield u.col
              // calculate the range of unavailable colors
              val (from, to) = colors.fold[List[(Cid, Cid)]](
                Nil, col => col -> col :: Nil, merge(_, _)).head
              // update with the minimal free color
              val col = if (from > minCol) minCol else next(to)
              v.col   = col
              // send the new color to neighbors with greater ID
              for (Edge(src, dst) <- directed; if src == v.id)
                yield Message(src, dst, col)
            } else DataBag()
          })

        colored = for (Vertex(id, col) <- vertices.bag())
          yield ColoredVertex(id, col)
      }

      write(output, new CSVOutputFormat[ColoredVertex]) { colored }
    }

    //algorithm run rt
  }
}

object GraphColoring {
  class Command extends Algorithm.Command[GraphColoring] {
    import Command._

    override def name = "gc"

    override def description =
      "Compute a greedy graph coloring scheme."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(keyInput)
        .`type`(classOf[String])
        .dest(keyInput)
        .metavar("INPUT")
        .help("graph edges")

      parser.addArgument(keyOutput)
        .`type`(classOf[String])
        .dest(keyOutput)
        .metavar("OUTPUT")
        .help("colored vertices")
    }
  }

  object Command {
    val keyInput  = "input"
    val keyOutput = "output"
  }

  object Schema {
    import Ordering.Implicits._
    import Numeric .Implicits._
    
    type Vid = Long
    type Cid = Int

    case class Vertex(@id id: Vid, var col: Cid = minCol)
        extends Identity[Vid] { def identity = id }

    case class ColoredVertex(@id id: Vid, col: Cid = minCol)
        extends Identity[Vid] { def identity = id }

    case class Edge(@id src: Vid, @id dst: Vid)
        extends Identity[(Vid, Vid)] { def identity = src -> dst }
    
    case class Message(@id src: Vid, @id dst: Vid, col: Cid = minCol)
        extends Identity[(Vid, Vid)] { def identity = src -> dst }

    val minCol: Cid = 0
    
    def next[N](prev: N)(implicit n: Numeric[N]): N =
      prev + n.fromInt(1)

    /** Compress a list of ranges maintaining order. */
    @tailrec def compress[N: Numeric](
        ranges: List[(N, N)],
        acc:    List[(N, N)] = Nil): List[(N, N)] =
      ranges match {
        case ((r1@(x1, y1)) :: (x2, y2) :: rs)
            if x1 <= x2 && y2 <= y1 =>
          compress(r1 :: rs, acc)
        case ((x1, y1) :: (r2@(x2, y2)) :: rs)
            if x2 <= x1 && y1 <= y2 =>
          compress(r2 :: rs, acc)
        case ((x1, y1) :: (x2, y2) :: rs)
            if x2 <= next(y1) =>
          compress(x1 -> y2 :: rs, acc)
        case (r :: rs) =>
          compress(rs, r :: acc)
        case _ =>
          acc.reverse
      }

    /** Merge 2 lists of ranges into 1 maintaining order and compression. */
    @tailrec def merge[N: Numeric](
        ranges1: List[(N, N)],
        ranges2: List[(N, N)],
        acc:     List[(N, N)] = Nil): List[(N, N)] =
      (ranges1, ranges2) match {
        case ((r1@(x1, y1)) :: rs1, (x2, y2) :: rs2)
            if x1 <= x2 && y2 <= y1 =>
          merge(ranges1, rs2, acc)
        case ((x1, y1) :: rs1, (r2@(x2, y2)) :: rs2)
            if x2 <= x1 && y1 <= y2 =>
          merge(rs1, ranges2, acc)
        case ((x1, y1) :: rs1, (x2, y2) :: rs2)
            if x1 <= x2 && x2 <= next(y1) =>
          merge(rs1, x1 -> y2 :: rs2, acc)
        case ((x1, y1) :: rs1, (x2, y2) :: rs2)
            if x2 <= x1 && x1 <= next(y2) =>
          merge(x2 -> y1 :: rs1, rs2, acc)
        case ((r1@(_, y1)) :: rs1, (x2, _) :: _)
            if x2 > y1 =>
          merge(rs1, ranges2, r1 :: acc)
        case (_, r2 :: rs2) =>
          merge(ranges1, rs2, r2 :: acc)
        case (_, Nil) =>
          acc.reverse ::: ranges1
        case (Nil, _) =>
          acc.reverse ::: ranges2
        case _ =>
          acc.reverse
      }
  }
}
