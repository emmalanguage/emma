package eu.stratosphere.emma.macros.program.controlflow

import scala.collection.Set
import scala.collection.mutable.ListBuffer

private[emma] trait ControlFlowCompiler extends ControlFlowModel {
  import universe._

  def compileDriver[T: c.WeakTypeTag](cfGraph: CFGraph): ListBuffer[Tree] = {
    // build driver tree startin from the (per construction unique) head block

    cfGraph.nodes.find(_.inDegree == 0) match {
      case Some(head) =>
        buildDriverTree(head, cfGraph)
      case None =>
        c.error(c.enclosingPosition, "Cannot determine head of CF graph")
        ListBuffer.empty[Tree]
    }
  }

  def compileBlock(block: CFBlock) = {
    val defName = block.defName
    val defType = block.defType
    val defBody = if (block.stats.last.tpe != NoType) q"{ ..${block.stats.toList} }" else q"{ ..${block.stats.toList}; Unit }"

    q"def $defName: $defType = $defBody"
  }

  private def buildDriverTree(head: CFGraph#NodeT, graph: CFGraph): ListBuffer[Tree] = {
    // outer object for the head
    val headBlock = head.value.asInstanceOf[CFBlock]
    // initialize result buffer
    val stats = ListBuffer[Tree]()

    // recursive descent based on the block type
    headBlock.kind match {

      case Linear =>
        stats ++= headBlock.stats
        stats ++= (findByLabel(head, label = true) match {
          case Some(next) => buildDriverTree(next, graph - head)
          case _ => ListBuffer.empty[Tree]
        })

      case Cond =>
        // get the `then` and `else` branch head nodes
        val optThenHead = findByLabel(head, label = true)
        val optElseHead = findByLabel(head, label = false)

        // get the `then` and `else` branch descendants
        val thenBlocks = for (x <- optThenHead match {
          case Some(thenHead) => thenHead.innerNodeTraverser.toSet
          case _ => Set.empty[CFGraph#NodeT]
        }) yield x.value.asInstanceOf[CFBlock]
        val elseBlocks = for (x <- optElseHead match {
          case Some(elseHead) => elseHead.innerNodeTraverser.toSet
          case _ => Set.empty[CFGraph#NodeT]
        }) yield x.value.asInstanceOf[CFBlock]
        // compute the intersection between the two descendant sets
        val commonBlocks = thenBlocks intersect elseBlocks

        // restrict `then`, `else` and `next` subgraphs
        val thenGraph = graph.clone()
        for (x <- thenGraph.nodes; y <- Some(x.value.asInstanceOf[CFBlock]); if !thenBlocks.contains(y) || commonBlocks.contains(y)) thenGraph -= x
        val elseGraph = graph.clone()
        for (x <- elseGraph.nodes; y <- Some(x.value.asInstanceOf[CFBlock]); if !elseBlocks.contains(y) || commonBlocks.contains(y)) elseGraph -= x
        val nextGraph = graph.clone()
        for (x <- nextGraph.nodes; y <- Some(x.value.asInstanceOf[CFBlock]); if !commonBlocks.contains(y)) nextGraph -= x

        // build `then` and `else` driver subtrees
        val thenTree = optThenHead match {
          case Some(thenHead) => buildDriverTree(thenGraph.get(thenHead.value.asInstanceOf[CFBlock]), thenGraph)
          case _ => ListBuffer[Tree]()
        }
        val elseTree = optElseHead match {
          case Some(elseHead) => buildDriverTree(elseGraph.get(elseHead.value.asInstanceOf[CFBlock]), elseGraph)
          case _ => ListBuffer[Tree]()
        }

        // FIXME: handle conditionals without else branch
        // build if (cond) thenp else elsep driver dubtree
        stats += q"if ({ ..${headBlock.stats.toList} }) { ..${thenTree.toList} } else { ..${elseTree.toList} }"

        // build `next` driver subtrees
        for (head <- nextGraph.nodes if head.inDegree == 0)
          stats ++= buildDriverTree(head, nextGraph)

      case WhileBegin(i) =>
        // get the head and the tail of the `body` branch
        val optBodyHead = findByLabel(head, label = true)
        val optBodyTail = graph.nodes find (_.value.asInstanceOf[CFBlock].kind match {
          case WhileEnd(j) if i == j => true
          case _ => false
        })
        // get the head of the `next` branch
        val optNextHead = findByLabel(head, label = false)

        // get the blocks in the `body` branch
        val bodyBlocks = {
          // compute all inner nodes between the head and the tail of the body (inclusive)
          val nodes = for (h <- optBodyHead;
                           t <- optBodyTail;
                           x <- Some(h.asInstanceOf[CFGraph#NodeT].innerNodeTraverser.toSet);
                           y <- Some(t.asInstanceOf[CFGraph#NodeT].innerNodeTraverser.toSet)) yield (x diff y) union Set(t.asInstanceOf[CFGraph#NodeT])
          // transform to outer nodes
          for (n <- nodes.get) yield n.value.asInstanceOf[CFBlock]
        }
        // get the blocks in the `next` branch
        val nextBlocks = {
          // compute all inner for the `next` subgraph
          val nodes = for (n <- optNextHead; x <- Some(n.innerNodeTraverser.toSet)) yield x
          // transform to outer nodes
          for (n <- nodes.get) yield n.value.asInstanceOf[CFBlock]
        }

        // restrict `body` and `next` subgraphs
        val bodyGraph = graph.clone()
        for (n <- bodyGraph.nodes; y <- Some(n.value.asInstanceOf[CFBlock]); if !bodyBlocks.contains(y)) bodyGraph -= n
        val nextGraph = graph.clone()
        for (n <- nextGraph.nodes; y <- Some(n.value.asInstanceOf[CFBlock]); if !nextBlocks.contains(y)) nextGraph -= n

        // build `body` and `next` driver subtrees
        val bodyTree = optBodyHead match {
          case Some(bodyHead) => buildDriverTree(bodyGraph.get(bodyHead.value.asInstanceOf[CFBlock]), bodyGraph)
          case _ => ListBuffer[Tree]()
        }
        val nextTree = optNextHead match {
          case Some(nextHead) => buildDriverTree(nextGraph.get(nextHead.value.asInstanceOf[CFBlock]), nextGraph)
          case _ => ListBuffer[Tree]()
        }

        // FIXME: changes semantics if condition test was pulled to the end
        stats += q"while ({ ..${headBlock.stats.toList} }) { ..${bodyTree.toList} }"
        stats ++= nextTree

      case WhileEnd(i) =>
        stats ++= headBlock.stats

      case DoWhileBegin(i) =>
        c.error(c.enclosingPosition, "Unsupported CFBLock type: DoWhileBegin")

      case DoWhileEnd(i) =>
        stats ++= headBlock.stats

      case _ =>
        c.error(c.enclosingPosition, "Unknown CFBLock type")
    }

    // the compiled sequence of statements
    stats
  }
}
