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
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

class MinimumSpanningForest(inputUrl: String, outputUrl: String, rt: Engine) extends Algorithm(rt) {

  import MinimumSpanningForest.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](MinimumSpanningForest.Command.KEY_INPUT),
    ns.get[String](MinimumSpanningForest.Command.KEY_OUTPUT),
    rt)

  def run() = {

    val algorithm = /*emma.parallelize*/ {

      val edges = DataBag(
        List(
          EdgeWithLabel(1, 2, 3.0),
          EdgeWithLabel(1, 3, 5.0),
          EdgeWithLabel(1, 4, 7.0),
          EdgeWithLabel(2, 4, 2.0),
          EdgeWithLabel(1, 5, 4.0),
          EdgeWithLabel(6, 7, 5.0),
          EdgeWithLabel(7, 8, 9.0),
          EdgeWithLabel(9, 8, 1.0)))

      // read input
      // val edges = read(inputUrl, new CSVInputFormat[EdgeWithLabel[Int, Double]])

      val V = (edges.map(_.src) plus edges.map(_.dst)).distinct() // re-construct V
      var S = DataBag[EdgeWithLabel[Int, Double]]()

      while (V.nonEmpty) {
        //---------------------------------------------------------------------
        // Phase 1. min-edge picking (TODO: resolve ties)
        //---------------------------------------------------------------------
        val Emin = for (g <- edges.groupBy(_.src))
          yield g.values.min(comparing { (x, y) => x.label < y.label })

        // extend S
        S = S plus Emin

        //---------------------------------------------------------------------
        // Phase 2. supervertex finding
        //---------------------------------------------------------------------
        val supervertices = stateful[VertexWithLabel[Int, SupervertexFindingState[Int]], Int]({
          // initialize state with unknown type and the min-edge destination as pointer
          for (e <- S) yield VertexWithLabel(e.src, SupervertexFindingState(Unknown, e.dst))
        })
        // create initial questions
        var questions = for (d <- supervertices.bag()) yield QuestionMsg(d.label.pointer, d.id)
        // initial answer phase: send messages to pointers
        val answers = (supervertices updateWithMany questions)(q => q.receiver, (s, questions) => {
          if (questions.map(_.sender).exists(_ == s.label.pointer)) {
            // an incoming question message was sent by by the current supervertex pointer (conjoined-tree root case)
            s.label.pointer = s.id
            s.label.tpe = if (s.id < s.label.pointer) Supervertex else PointsAtSupervertex // change type
          }
          // the answer contains the current pointer and a boolean flag indicating whether this pointer is a supervertex or not
          for (q <- questions) yield AnswerMsg(q.sender, s.label.pointer, s.label.tpe == Supervertex)
        })

        while (supervertices.bag().exists(_.label.tpe == Unknown)) {
          // question phase: merge answers and generate next questions
          questions = (supervertices updateWithMany answers)(a => a.receiver, (s, answers) => {
            val supervertex = answers.fetch().collectFirst({
              case x if x.pointerIsSupervertex => x
            })
            if (supervertex.isDefined) {
              s.label.tpe = PointsAtSupervertex
              s.label.pointer = supervertex.get.pointer
            }
            DataBag(Seq.empty[QuestionMsg[Int]])
          })
        }
      }



      //      // initialize delta and state
      //      var delta = for (v <- vertices) yield State(v.id, v.neighborIDs, v.id)
      //      val state = stateful[State, VID](delta)
      //
      //      // forward propagation
      //      while (!delta.empty()) {
      //        // send messages to neighbors
      //        val messages = for (
      //          s <- delta;
      //          n <- s.neighborIDs) yield Message(n, s.component)
      //
      //        // aggregate updates from incoming messages
      //        val updates = for (g <- messages.groupBy(m => m.receiver)) yield Update(g.key, g.values.map(_.component).max())
      //
      //        // update state
      //        delta = state.update(updates)((s, u) => if (u.component > s.component) Some(s.copy(component = u.component)) else None)
      //      }
      //
      //      // construct components from state
      //      val components = for (s <- state.bag()) yield Component(s.vertexID, s.component)
      //
      //      // write result
      //      write(outputUrl, new CSVOutputFormat[Component])(components)
      //
      //      println(components.fetch())
    }

    //algorithm.run(rt)
  }
}

object MinimumSpanningForest {

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
  }

  class Command extends Algorithm.Command[MinimumSpanningForest] {

    // algorithm names
    override def name = "sssp"

    override def description = "parallel version of Boruvka's MSF algorithm"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      // add arguments
      parser.addArgument(Command.KEY_INPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_INPUT)
        .metavar("EDGES")
        .help("edges file (\"<src>TAB<dst>\" format expected")
      parser.addArgument(Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file (minimum spanning forest)")
    }
  }

  // --------------------------------------------------------------------------------------------
  // ----------------------------------- tpch -------------------------------------------------
  // --------------------------------------------------------------------------------------------

  object Schema {
    val Unknown = 0.toShort
    val Supervertex = 1.toShort
    val PointsAtSupervertex = 2.toShort

    case class SupervertexFindingState[VT](var tpe: Short, var pointer: VT)

    case class QuestionMsg[VT](receiver: VT, sender: VT) {}

    case class AnswerMsg[VT](receiver: VT, pointer: VT, pointerIsSupervertex: Boolean) {}

  }

}

