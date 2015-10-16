package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

object ConnectedComponents {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
  }

  class Command extends Algorithm.Command[ConnectedComponents] {

    // algorithm names
    override def name = "cc"

    override def description = "Compute the connected components of a graph"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      // add arguments
      parser.addArgument(Command.KEY_INPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_INPUT)
        .metavar("GRAPH")
        .help("graph file")
      parser.addArgument(Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("components file ")
    }
  }

  object Schema {

    type VID = Long

    case class Vertex(@id id: VID, neighborIDs: DataBag[VID]) extends Identity[VID] {
      def identity = id
    }

    case class Message(receiver: VID, component: VID) {}

    case class Update(@id id: VID, component: VID) extends Identity[VID] {
      def identity = id
    }

    case class State(
        @id vertexID:  VID,
        /*
        //nested version
        var neighbors: DataBag[VID],
        */
        var component: VID) extends Identity[VID] {
      def identity = vertexID
    }

    case class Component(@id vertexID: VID, component: VID) extends Identity[VID] {
      def identity = vertexID
    }

  }

}

class ConnectedComponents(inputUrl: String, outputUrl: String, rt: Engine) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.graphs.ConnectedComponents.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](ConnectedComponents.Command.KEY_INPUT),
    ns.get[String](ConnectedComponents.Command.KEY_OUTPUT),
    rt)

  def run() = algorithm run rt

  val algorithm = emma.parallelize {

    /*
    //nested version
    val vertices = DataBag(
      List(
        Vertex(1, DataBag(Seq(2, 5, 7))),
        Vertex(2, DataBag(Seq(1, 5, 7))),
        Vertex(3, DataBag(Seq(3))),
        Vertex(4, DataBag(Seq(6))),
        Vertex(5, DataBag(Seq(1, 2))),
        Vertex(6, DataBag(Seq(4))),
        Vertex(7, DataBag(Seq(1, 2)))))
    */

    val edges: DataBag[Edge[VID]] = DataBag(Seq(
      Edge(1,2), Edge(1,5), Edge(1,7),
      Edge(2,1), Edge(2,5), Edge(2,7),
      Edge(3,3),
      Edge(4,6),
      Edge(5,1), Edge(5,2),
      Edge(6,4),
      Edge(7,1),
      Edge(7,2)
    ))

    val vertices = (edges.map(_.src) plus edges.map(_.dst)).distinct()

    // initialize delta and state
    /*
    //nested version
    var delta = for (v <- vertices) yield State(v.id, v.neighborIDs, v.id)
    */
    var delta = for (v <- vertices) yield State(v, v)
    val state = stateful[State, VID](delta)

    // forward propagation
    while (delta.nonEmpty) {
      // send messages to neighbors
      /*
      //nested version
      val messages = for {
        state <- delta
        n <- state.neighbors
      } yield Message(n, state.component)
      */
      val messages = for {
        state <- delta
        edge <- edges
        if state.vertexID == edge.src
      } yield {
        Message(edge.dst, state.component)
      }

      // aggregate updates from incoming messages
      val updates = for (g <- messages groupBy { _.receiver })
        yield Update(g.key, g.values.map { _.component }.max())

      // update state
      delta = state.updateWithOne(updates)(_.id, (state, update) =>
        if (update.component > state.component) {
          state.component = update.component
          DataBag(state :: Nil)
        } else DataBag[State]())
    }

    // construct components from state
    val components = for (s <- state.bag()) yield Component(s.vertexID, s.component)

    // write result
    //write(outputUrl, new CSVOutputFormat[Component])(components)

    components.fetch()
  }
}

