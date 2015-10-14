package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.util.Counter
import scalax.collection.GraphPredef._
import scalax.collection.edge.LkDiEdge
import scalax.collection.mutable.Graph

private[emma] trait ControlFlowAnalysis extends ControlFlowModel with ControlFlowNormalization {
  import universe._

  def createControlFlowGraph(tree: Tree): CFGraph = {

    // Loop ID counter
    val loopCounter = new Counter()

    // Vertex ID counter and an implicit next nodeID provider
    val vertexCounter = new Counter()
    implicit def nextVertexID = vertexCounter.advance.get

    // Initialize graph
    val curV  = new CFBlock()
    val graph = Graph[CFBlock, LkDiEdge](curV)

    // Recursive helper function
    def createCFG(curTree: Tree, curBlock: CFBlock): CFBlock = curTree match {
      // { `stats`; `expr` }
      case Block(stats, expr) =>
        var block = curBlock
        for (stmt <- stats) block = createCFG(stmt, block)
        createCFG(expr, block)

      case ValDef(_, _, _,  _ @ (_: Block | _: If))
        |  Assign(_: Ident, _ @ (_: Block | _: If))
        => c.abort(curTree.pos,
            "Emma does not support assignments with conditionals on the rhs at the moment")

      case _: ValDef | Assign(_: Ident, _) =>
        // Add assignment to the current block
        curBlock.stats += curTree
        curBlock

      case q"do $body while (${cond: Ident})" =>
        // Create body block
        val startBlock = new CFBlock()
        val endBlock   = createCFG(body, startBlock)

        // Create condition block with the `cond` term
        val condBlock = new CFBlock()
        condBlock.stats += cond

        // Create next block
        val nextBlock = new CFBlock()

        // Fix block type
        val loopID      = loopCounter.advance.get
        startBlock.kind = DoWhileBegin(loopID)
        endBlock.kind   = DoWhileEnd(loopID)

        graph += LkDiEdge(curBlock,  startBlock)(true)
        graph += LkDiEdge(endBlock,  condBlock)(true)
        graph += LkDiEdge(condBlock, startBlock)(true)
        graph += LkDiEdge(condBlock, nextBlock)(false)

        nextBlock

      case q"while (${cond: Ident}) $body" =>
        // Create condition block with the `cond` term
        val condBlock = new CFBlock()
        condBlock.stats += cond

        // Create body block
        val startBlock = new CFBlock()
        val endBlock   = createCFG(body, startBlock)

        // Create next block
        val nextBlock = new CFBlock()

        // Fix block type
        val loopID = loopCounter.advance.get

        // Fix block kinds
        condBlock.kind = WhileBegin(loopID)
        endBlock.kind  = WhileEnd(loopID)

        graph += LkDiEdge(curBlock,  condBlock)(true)
        graph += LkDiEdge(condBlock, startBlock)(true)
        graph += LkDiEdge(condBlock, nextBlock)(false)
        graph += LkDiEdge(endBlock,  condBlock)(true)

        nextBlock

      case q"if (${cond: Ident}) $thn else $els" =>
        // Create condition block with the `cond` term
        val condBlock = new CFBlock()
        condBlock.stats += cond

        // Create thn block
        val thnStartBlock = new CFBlock()
        val thnEndBlock   = createCFG(thn, thnStartBlock)

        // Create els block
        val elsStartBlock = new CFBlock()
        val elsEndBlock   = createCFG(els, elsStartBlock)

        // Create new current block
        val curNewBlock = new CFBlock()

        // Fix block kind
        condBlock.kind = Cond

        graph += LkDiEdge(condBlock,   condBlock)(true)
        graph += LkDiEdge(condBlock,   thnStartBlock)(true)
        graph += LkDiEdge(condBlock,   elsStartBlock)(false)
        graph += LkDiEdge(thnEndBlock, curNewBlock)(true)
        graph += LkDiEdge(elsEndBlock, curNewBlock)(true)

        curNewBlock

      // Default: a term expression
      case _ =>
        curBlock.stats += curTree
        curBlock
    }

    // Call recursive helper
    createCFG(tree, curV)

    // Remove empty blocks from the end of the graph
    // (might result from tailing if (c) e1 else e2 return statements)
    var emptyBlocks = graph.nodes filter { n => n.diSuccessors.isEmpty && n.stats.isEmpty }
    while (emptyBlocks.nonEmpty) {
      // Remove empty block with no successors from the graph
      for (block <- emptyBlocks) graph -= block
      emptyBlocks = graph.nodes filter { n => n.diSuccessors.isEmpty && n.stats.isEmpty }
    }

    // Return the graph
    graph
  }
}
