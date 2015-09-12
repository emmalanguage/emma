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

      case ValDef(_, _, _, rhs) =>
        // Ensure that the `rhs` tree is a "simple" term (no blocks or conditionals)
        if (rhs match {
          case _: Block => true
          case _: If    => true
          case _        => false
        }) c.abort(curTree.pos,
          "Emma does not support assignments with conditionals on the rhs at the moment")

        // Add assignment to the current block
        curBlock.stats += curTree
        curBlock

      // `name` = `rhs`
      case Assign(_: Ident, rhs) =>
        // Ensure that the `rhs` tree is a "simple" term (no blocks or conditionals)
        if (rhs match {
          case _: Block => true
          case _: If    => true
          case _        => false
        }) c.abort(curTree.pos,
          "Emma does not support assignments with conditionals on the rhs at the moment")

        // Add assignment to the current block
        curBlock.stats += curTree
        curBlock

      // do  { `body` } while (`cond`)
      case LabelDef(_, _, Block(b, If(cond, _, _))) =>
        // Ensure that the `cond` tree is a simple identifier
        // Should not happen after normalization
        if (cond match {
          case _: Ident => false
          case _        => true
        }) c.abort(curTree.pos,
          s"Invalid do-while test expression: ${showCode(cond)}")

        // Create body block
        val body       = if (b.size > 1) Block(b.init, b.last) else b.head
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

      // while (`cond`) { `body` }
      case LabelDef(_, _, If(cond, Block(b, _), _)) =>
        // Ensure that the `cond` tree is a simple identifier
        // Should not happen after normalization
        if (cond match {
          case _: Ident => false
          case _        => true
        }) c.abort(curTree.pos,
          s"Invalid while test expression: ${showCode(cond)}")

        // Create condition block with the `cond` term
        val condBlock = new CFBlock()
        condBlock.stats += cond

        // Create body block
        val body       = if (b.size > 1) Block(b.init, b.last) else b.head
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

      // if (`cond`) `thn` else `els`
      case If(cond, thn, els) =>
        // Ensure that the `cond` tree is a simple identifier
        // Should not happen after normalization
        if (cond match {
          case _: Ident => false
          case _        => true
        }) c.abort(curTree.pos,
          s"Invalid if-then-else test expression: ${showCode(cond)}")

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
