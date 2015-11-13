package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

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
      val neighborsT = (for {
        v <- vertices
        n <- v.neighborIDs
      } yield n -> v.id) groupBy { _._1 }

      val state = stateful[State, VID](for {
        v <- vertices
        n <- neighborsT
        if n.key == v.id
      } yield State(v.id, v.neighborIDs, n.values map { _._2 }, v.id))

      var i = 1
      while (state.bag().exists(_.active)) {
        println(f"iteration #$i%05d")


        {
          // trimming
          println(" - trimming")

          state updateWithZero { state =>
            if (state.active) {
              state.component = state.vertexID
              state.active    = state.neighbors.nonEmpty || state.neighborsT.nonEmpty
            }

            DataBag()
          }
        }

        {
          // forward traversal
          println(" - forward traversal")

          var changed = true
          while (changed) {
            // send messages to neighbors
            val messages = for {
              state <- state.bag()
              if state.active
              n <- state.neighbors
            } yield Message(n, state.component)

            // aggregate updates from incoming messages
            val updates = for (g <- messages groupBy { _.receiver })
              yield UpdateComponent(g.key, g.values.map { _.payload }.max())

            // update state
            val delta = state.updateWithOne(updates)(_.id, (state, update) =>
              if (update.component > state.component) {
                state.component = update.component
                DataBag(state :: Nil)
              } else DataBag())

            changed = delta.nonEmpty
          }
        }

        {
          // backward traversal
          println(" - backward traversal")

          // send messages to neighbors of wcc roots
          var updates = for {
            state <- state.bag()
            if state.active
            if state.vertexID == state.component
            n <- state.neighborsT
          } yield UpdateComponent(n, state.component)

          // mark wcc "roots" as inactive
          state updateWithZero { state =>
            if (state.active && state.vertexID == state.component)
              state.active = false

            DataBag()
          }

          var convergedVertexExists = true
          while (convergedVertexExists) {
            // compute state delta
            val delta = state.updateWithOne(updates)(_.id, (state, update) =>
              if (state.active && state.component == update.component) {
                state.active = false
                DataBag(state :: Nil)
              } else DataBag())

            // compute new updates
            updates = for (d <- delta; n <- d.neighborsT)
              yield UpdateComponent(n, d.component)

            // update termination condition
            convergedVertexExists = delta.nonEmpty
          }
        }

        // neighbours pruning phase
        {
          // graph prunig
          println(" - graph prunig")

          val neighbors = (for {
            state <- state.bag()
            if state.active
            n <- state.neighborsT
          } yield Message(n, state.vertexID)) groupBy { _.receiver }

          state.updateWithOne(neighbors)(_.key, (state, update) =>
            if (state.active) {
              state.neighbors = update.values map { _.payload }
              DataBag(state :: Nil)
            } else DataBag())

          val neighborsT = (for {
            state <- state.bag()
            if state.active
            n <- state.neighborsT
          } yield Message(n, state.vertexID)) groupBy { _.receiver }

          state.updateWithOne(neighborsT)(_.key, (state, update) =>
            if (state.active) {
              state.neighborsT = update.values map { _.payload }
              DataBag(state :: Nil)
            } else DataBag())
        }

        i += 1
      }

      // construct components from state
      val components = for (state <- state.bag())
        yield Component(state.vertexID, state.component)

      // write result
      write(outputUrl, new CSVOutputFormat[Component])(components)

      //println(components.fetch())
    }

    //algorithm.run(rt)
  }
}

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
  // ----------------------------------- tpch -------------------------------------------------
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

    case class State(
          @id vertexID:   VID,
          var neighbors:  DataBag[VID],
          var neighborsT: DataBag[VID],
          var component:  VID,
          var active:     Boolean = true)
        extends Identity[VID] {
      def identity = vertexID
    }

    case class Component(@id vertexID: VID, component: VID) extends Identity[VID] {
      def identity = vertexID
    }

  }

}

