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
package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.macros.BlackBoxUtil
import scala.collection.Set
import scala.collection.mutable.ListBuffer

private[emma] trait ControlFlowCompiler extends ControlFlowModel with BlackBoxUtil {
  import universe._
  import syntax._

  def compileDriver[T: c.WeakTypeTag](cfGraph: CFGraph): ListBuffer[Tree] = {
    // Build driver tree starting from the (per construction unique) head block
    cfGraph.nodes find { _.inDegree == 0 } match {
      case Some(head) => buildDriverTree(head, cfGraph)
      case None       => c.abort(c.enclosingPosition, "Cannot determine head of CF graph")
    }
  }

  def compileBlock(block: CFBlock) = {
    val defName = block.defName
    val defType = block.defType
    val stats   = block.stats
    val defBody =
      if (stats.last.tpe != NoType) q"{ ..$stats }"
      else q"{ ..$stats; () }"

    q"def $defName: $defType = $defBody"
  }

  private def buildDriverTree(head: CFGraph#NodeT, graph: CFGraph): ListBuffer[Tree] = {
    // Outer object for the head
    val headBlock = head.value.as[CFBlock]
    // Initialize result buffer
    val stats = ListBuffer.empty[Tree]

    // Recursive descent based on the block type
    headBlock.kind match {

      case Linear =>
        stats ++= headBlock.stats
        stats ++= (findByLabel(head, label = true) match {
          case Some(next) => buildDriverTree(next, graph - head)
          case None       => ListBuffer.empty[Tree]
        })

      case Cond =>
        // Get the `then` and `else` branch head nodes
        val optThenHead = findByLabel(head, label = true)
        val optElseHead = findByLabel(head, label = false)

        // Get the `then` and `else` branch descendants

        val thenBlocks =
          for (node <- optThenHead map { _.innerNodeTraverser.toSet } getOrElse Set.empty)
            yield node.value.as[CFBlock]

        val elseBlocks =
          for (node <- optElseHead map { _.innerNodeTraverser.toSet } getOrElse Set.empty)
            yield node.value.as[CFBlock]

        // Compute the intersection between the two descendant sets
        val commonBlocks = thenBlocks & elseBlocks

        // Restrict `then`, `else` and `next` sub-graphs

        val thenGraph = graph.clone()

        for {
          node <- thenGraph.nodes
          block = node.value.as[CFBlock]
          if !thenBlocks.contains(block) || commonBlocks.contains(block)
        } thenGraph -= node

        val elseGraph = graph.clone()

        for {
          node <- elseGraph.nodes
          block = node.value.as[CFBlock]
          if !elseBlocks.contains(block) || commonBlocks.contains(block)
        } elseGraph -= node

        val nextGraph = graph.clone()

        for {
          node <- nextGraph.nodes
          block = node.value.as[CFBlock]
          if !commonBlocks.contains(block)
        } nextGraph -= node

        // Build `then` and `else` driver subtrees

        val thenTree = optThenHead map { head =>
          buildDriverTree(thenGraph get head.value.as[CFBlock], thenGraph)
        } getOrElse ListBuffer.empty

        val elseTree = optElseHead map { head =>
          buildDriverTree(elseGraph get head.value.as[CFBlock], elseGraph)
        } getOrElse ListBuffer.empty

        // FIXME: Handle conditionals without else branch
        // Build if (cond) thn else els driver subtree
        stats += q"if ({ ..${headBlock.stats} }) { ..$thenTree } else { ..$elseTree }"

        // Build `next` driver subtrees
        for (head <- nextGraph.nodes if head.inDegree == 0)
          stats ++= buildDriverTree(head, nextGraph)

      case WhileBegin(i) =>
        // Get the head and the tail of the `body` branch
        val optBodyHead = findByLabel(head, label = true)
        val optBodyTail = graph.nodes find {
          _.value.as[CFBlock].kind match {
            case WhileEnd(j) => i == j
            case _ => false
          }
        }

        // Get the head of the `next` branch
        val optNextHead = findByLabel(head, label = false)

        // Get the blocks in the `body` branch
        val bodyBlocks = (for {
          head <- optBodyHead.toSeq
          tail <- optBodyTail.toSeq
          xs = head.as[CFGraph#NodeT].innerNodeTraverser.toSet
          ys = tail.as[CFGraph#NodeT].innerNodeTraverser.toSet
          node <- (xs diff ys) + tail.as[CFGraph#NodeT]
        } yield node.value.as[CFBlock]).toSet

        // Get the blocks in the `next` branch
        val nextBlocks = {
          // Compute all inner for the `next` sub-graph
          val nodes = for (node <- optNextHead) yield node.innerNodeTraverser.toSet
          // Transform to outer nodes
          for (node <- nodes.get) yield node.value.as[CFBlock]
        }

        // Restrict `body` and `next` sub-graphs

        val bodyGraph = graph.clone()

        for {
          node <- bodyGraph.nodes
          block = node.value.as[CFBlock]
          if !bodyBlocks.contains(block)
        } bodyGraph -= node

        val nextGraph = graph.clone()

        for {
          node <- nextGraph.nodes
          block = node.value.as[CFBlock]
          if !nextBlocks.contains(block)
        } nextGraph -= node

        // Build `body` and `next` driver subtrees

        val bodyTree = optBodyHead map { head =>
          buildDriverTree(bodyGraph get head.value.as[CFBlock], bodyGraph)
        } getOrElse ListBuffer.empty

        val nextTree = optNextHead map { head =>
          buildDriverTree(nextGraph get head.value.as[CFBlock], nextGraph)
        } getOrElse ListBuffer.empty

        // FIXME: Changes semantics if condition test was pulled to the end
        stats += q"while ({ ..${headBlock.stats} }) { ..$bodyTree }"
        stats ++= nextTree

      case WhileEnd(_) =>
        stats ++= headBlock.stats

      case DoWhileBegin(_) =>
        c.abort(c.enclosingPosition, "Unsupported CFBLock type: DoWhileBegin")

      case DoWhileEnd(_) =>
        stats ++= headBlock.stats

      case _ =>
        c.abort(c.enclosingPosition, "Unknown CFBLock type")
    }

    // The compiled sequence of statements
    stats
  }
}
