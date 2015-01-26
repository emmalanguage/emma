package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

object StronglyConnectedComponents {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
  }

  class Command extends Algorithm.Command[StronglyConnectedComponents] {

    // algorithm names
    override def name = "scc"

    override def description = "Compute the strongly connected components of a graph"

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

  // --------------------------------------------------------------------------------------------
  // ----------------------------------- Schema -------------------------------------------------
  // --------------------------------------------------------------------------------------------

  object Schema {

    type VID = Char

    case class Vertex(@id id: VID, neighborIDs: DataBag[VID]) extends Identity[VID] {
      def identity = id
    }

    case class Message(receiver: VID, payload: VID) {}

    case class UpdateComponent(@id id: VID, component: VID) extends Identity[VID] {
      def identity = id
    }

    case class UpdateNeighbors(@id id: VID, neighbors: DataBag[VID]) extends Identity[VID] {
      def identity = id
    }

    case class State(@id vertexID: VID, neighborIDs: DataBag[VID], transposeNeigborIDs: DataBag[VID], component: VID, active: Boolean = true) extends Identity[VID] {
      def identity = vertexID
    }

    case class Component(@id vertexID: VID, component: VID) extends Identity[VID] {
      def identity = vertexID
    }

  }

}

class StronglyConnectedComponents(inputUrl: String, outputUrl: String, rt: Engine) extends Algorithm(rt) {

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](StronglyConnectedComponents.Command.KEY_INPUT),
    ns.get[String](StronglyConnectedComponents.Command.KEY_OUTPUT),
    rt)

  def run() = {

    import eu.stratosphere.emma.examples.graphs.StronglyConnectedComponents.Schema._

    val algorithm = /*emma.parallelize*/ {

      //val vertices = DataBag(
      //  List(
      //    Vertex('a', DataBag(Seq('b'))),
      //    Vertex('b', DataBag(Seq('c', 'e', 'f'))),
      //    Vertex('c', DataBag(Seq('d', 'g'))),
      //    Vertex('d', DataBag(Seq('c', 'h'))),
      //    Vertex('e', DataBag(Seq('a', 'f'))),
      //    Vertex('f', DataBag(Seq('g'))),
      //    Vertex('g', DataBag(Seq('f'))),
      //    Vertex('h', DataBag(Seq('d', 'g')))))

      // read input
      val vertices = read(inputUrl, new CSVInputFormat[Vertex])

      // initialize state
      val transposeNeighbors = (for (x <- vertices; y <- x.neighborIDs) yield (y, x.id)).groupBy(_._1)
      val state = stateful[State, VID] {
        for (
          n <- vertices;
          t <- transposeNeighbors; if t.key == n.id) yield State(n.id, n.neighborIDs, t.values.map(_._2), n.id)
      }

      var i = 1
      while (state.bag().exists(_.active)) {
        println(f"iteration #$i%05d")


        {
          // trimming
          println(" - trimming")

          state.update(s => if (s.active) Some(s.copy(component = s.vertexID, active = !(s.neighborIDs.empty() && s.transposeNeigborIDs.empty()))) else None)
        }

        {
          // forward traversal
          println(" - forward traversal")

          var changed = true
          while (changed) {
            // send messages to neighbors
            val messages = for (
              s <- state.bag(); if s.active;
              n <- s.neighborIDs) yield Message(n, s.component)

            // aggregate updates from incoming messages
            val updates = for (g <- messages.groupBy(m => m.receiver)) yield UpdateComponent(g.key, g.values.map(_.payload).max())

            // update state
            val delta = state.update(updates)((s: State, u: UpdateComponent) => if (u.component > s.component) Some(s.copy(component = u.component)) else None)
            changed = !delta.empty()
          }
        }

        {
          // backward traversal
          println(" - backward traversal")

          // send messages to neighbors of wcc roots
          var updates = for (
            s <- state.bag(); if s.active && s.vertexID == s.component;
            n <- s.transposeNeigborIDs) yield UpdateComponent(n, s.component)

          // mark wcc "roots" as inactive
          state.update(s => if (s.active && s.vertexID == s.component) Some(s.copy(active = false)) else None)

          var convergedVertexExists = true
          while (convergedVertexExists) {
            // compute state delta
            val delta = state.update(updates)((s, u) => if (s.active && s.component == u.component) Some(s.copy(active = false)) else None)

            // compute new updates
            updates = for (d <- delta; n <- d.transposeNeigborIDs) yield UpdateComponent(n, d.component)

            // update termination condition
            convergedVertexExists = !delta.empty()
          }
        }

        // neighbours pruning phase
        {
          // graph prunig
          println(" - graph prunig")

          val neighborIDs = (for (
            s <- state.bag(); if s.active;
            n <- s.transposeNeigborIDs) yield Message(n, s.vertexID)).groupBy(_.receiver)
          state.update(neighborIDs)((s, u) => if (s.active) Some(s.copy(neighborIDs = u.values map { v => v.payload})) else None)

          val transposeNeigborIDs = (for (
            s <- state.bag(); if s.active;
            n <- s.transposeNeigborIDs) yield Message(n, s.vertexID)).groupBy(_.receiver)
          state.update(transposeNeigborIDs)((s, u) => if (s.active) Some(s.copy(transposeNeigborIDs = u.values map { v => v.payload})) else None)
        }

        i = i + 1
      }

      // construct components from state
      val components = for (s <- state.bag()) yield Component(s.vertexID, s.component)

      // write result
      write(outputUrl, new CSVOutputFormat[Component])(components)

      //println(components.fetch())
    }

    //algorithm.run(rt)
  }
}

