package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.macros.BinaryLiterals
import eu.stratosphere.emma.runtime.Engine

import scala.language.postfixOps

/**
 * This calculates the game-theoretical value of every game state of Tic-tac-toe.
 * (Game-theoretical value of a game state: what will be the result of the game (draw or who wins,
 * and in how many moves), when starting from this position, assuming optimal players.)
 *
 * The algorithm is retrograde analysis (sometimes called backward induction).
 * It starts from the end-states (where the game is over), and calculates backwards:
 * at iteration d, it calculates those states that are d moves away from an end-state by optimal play.
 *
 * Note, that the game graph of Tic-tac-toe is acyclic, but this implementation would also work for games where
 * there are cycles in the game graph.
 **/

class TicTacToe(rt: Engine = eu.stratosphere.emma.runtime.default())
    extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.graphs.TicTacToe._

  def run() = algorithm run rt

  val algorithm = emma.parallelize
  {
    // All subsets (as bitsets) of the squares of the board
    val subsets = DataBag(0 until (1<<9))

    // Generate all game states.
    // The white and black stones can occupy any such subset of the board positions where there is no overlap
    // between white and black stones.
    val states = for {
      whites <- subsets
      blacks <- subsets
      if (whites & blacks) == 0
    } yield GameState(whites, blacks)

    // Initialize end states
    val statesWithValues0 = states.map(state => VertexWithValue(state, initEndState(state)))

    // Compute edges
    val edges = for {
      state <- states
      parent <- DataBag(parentStates(state))
    } yield Edge(state, parent)

    // Calculate in-degrees
    val allEdgeTargets = edges map {_.dst}
    val inDegrees = for {
      g <- allEdgeTargets.groupBy {x => x}
    } yield (g.key, g.values.count())

    // Init still undefined values to count(inDegree)
    val statesWithValues = for {
      vertex <- statesWithValues0
      inDegree <- inDegrees
      if vertex.id == inDegree._1
    } yield vertex.vc match {
        case Undefined() => VertexWithValue(vertex.id, Count(inDegree._2.asInstanceOf[Int]))
        case x => vertex
      }


    // The iteration
    val solution = stateful[VertexWithValue, GameState](statesWithValues)
    var valueChanges = statesWithValues withFilter {_.vc.isInstanceOf[Value]}
    var d: Int = 1

    while (!valueChanges.isEmpty) {

      println("Depth " ++ d.toString)

      val msgs = for {
        newVal <- valueChanges
        parent <- DataBag(parentStates(newVal.id))
      } yield Msg(parent, newVal.vc.asInstanceOf[Value])

      valueChanges = solution.updateWithMany(msgs)(
        _.target, (v, msgs) => {
          if (msgs.count > 0) {
            if (msgs.exists(_.value.isInstanceOf[Loss])) {
              v.vc match {
                case _: Win   =>
                  DataBag()
                case _: Count =>
                  v.vc = Win(d)
                  DataBag(Seq(v))
                case _        =>
                  assert(false)
                  DataBag()
              }
            } else {
              v.vc match {
                case count: Count =>
                  val newCount = count.count - msgs.count().asInstanceOf[Int]
                  if (newCount > 0) {
                    v.vc = Count(newCount)
                    DataBag()
                  } else {
                    assert(newCount == 0)
                    v.vc = Loss(d)
                    DataBag(Seq(v))
                  }
                case _ =>
                  assert(v.vc.isInstanceOf[Win] || initEndState(v.id) == Loss(0))
                  assert(!v.vc.isInstanceOf[Win] || v.vc.asInstanceOf[Win].depth <= d)
                  DataBag()
              }
            }
          } else {
            DataBag()
          }
        }
      )

      d += 1
    }

//    import java.lang.Integer.toBinaryString
//    write("/tmp/emma-output/TicTacToe.txt", new CSVOutputFormat[(String, String)]) (
//      for (v <- solution.bag()) yield (toBinaryString(v.id.whites) ++ " | " ++ toBinaryString(v.id.blacks), v.vc.toString))

    solution.bag()
  }
}

object TicTacToe {

  case class GameState(whites: Int, blacks: Int) // Bitsets for white and black stones (9 bits in each, as the board is 3x3)

  sealed trait ValueCount
    sealed trait Value extends ValueCount // values are understood from the point of view of the player to move
      case class Win(depth: Int) extends Value
      case class Loss(depth: Int) extends Value
    case class Count(count: Int) extends ValueCount // the number of successor states still unprocessed
    case class Undefined() extends ValueCount

  case class VertexWithValue(@id id: GameState, var vc: ValueCount)
    extends Identity[GameState] { def identity = id }

  case class Msg(target: GameState, value: Value)
  case class Update(state: GameState, value: Value, decCount: Int)


  //todo: why doesn't this work with parallelize if I move the following two functions to the TicTacToe class?
  // Move generation
  def parentStates(s: GameState) = {
    for {
      i <- 0 until 9 // go through all board positions
      if (s.blacks & (1<<i)) != 0 // blacks has bit i set
    } yield GameState(s.blacks & ~(1<<i), s.whites) // remove the stone at position i, and do state negation, by switching whites and blacks
  }

  // Decides whether the given state is an end-state. If it is, then it returns Win or Loss in 0 moves. If it is not, then it returns Undefined.
  def initEndState(state: GameState) = {

    def contains(s: Int, mask: Int) = (s & mask) == mask

    def hasThreeInARow(s: Int) = {
      import BinaryLiterals._

      contains(s, b"111 000 000") || contains(s, b"000 111 000") || contains(s, b"000 000 111") || // rows
        contains(s, b"100 100 100") || contains(s, b"010 010 010") || contains(s, b"001 001 001") || // columns
        contains(s, b"100 010 001") || contains(s, b"001 010 100") // diagonals
    }

    if(hasThreeInARow(state.whites)) // White won
      Win(0)
    else if(hasThreeInARow(state.blacks)) // Black won
      Loss(0)
    else Undefined() // not end-state
  }

}

