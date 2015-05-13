package eu.stratosphere.emma.macros.program.controlflow


import eu.stratosphere.emma.macros.program.util.ProgramUtils
import eu.stratosphere.emma.util.Counter

import scala.reflect.macros._
import scalax.collection.GraphPredef._
import scalax.collection.edge.LkDiEdge
import scalax.collection.mutable.Graph

private[emma] trait ControlFlowAnalysis[C <: blackbox.Context] extends ControlFlowModel[C] with ProgramUtils[C] {
  this: ControlFlowModel[C] =>

  import c.universe._

  def createControlFlowGraph(tree: Tree): CFGraph = {

    // loop ID counter
    val loopCounter = new Counter()

    // vertex ID counter and an implicit next nodeID provider
    val vertexCounter = new Counter()
    implicit def nextVertexID = vertexCounter.advance.get

    // initialize graph
    val vCurr = new CFBlock()
    val graph = Graph[CFBlock, LkDiEdge](vCurr)

    // recursive helper function
    def _createCFG(currTree: Tree, currBlock: CFBlock): CFBlock = currTree match {
      // { `stats`; `expr` }
      case Block(stats, expr) =>
        var _currBlock = currBlock
        for (s <- stats) {
          _currBlock = _createCFG(s, _currBlock)
        }
        _createCFG(expr, _currBlock)

      // val `name`: `tpt` = `rhs`
      case ValDef(mods, name, tpt, rhs) =>
        // ensure that the `rhs` tree is a "simple" term (no blocks or conditionals)
        if (rhs match {
          case Block(_, _) => true
          case If(_, _, _) => true
          case _ => false
        }) c.error(c.enclosingPosition, "Emma does not support assignments with conditionals on the rhs at the moment!")

        // add assignment to the current block
        currBlock.stats += currTree
        currBlock

      // `name` = `rhs`
      case Assign(Ident(name: TermName), rhs) =>
        // ensure that the `rhs` tree is a "simple" term (no blocks or conditionals)
        if (rhs match {
          case Block(_, _) => true
          case If(_, _, _) => true
          case _ => false
        }) c.error(c.enclosingPosition, "Emma does not support assignments with conditionals on the rhs at the moment!")

        // add assignment to the current block
        currBlock.stats += currTree
        currBlock

      // do  { `body` } while (`cond`)
      case LabelDef(_, _, Block(b, If(cond, _, _))) =>
        // ensure that the `cond` tree is a simple identifier
        if (cond match {
          case Ident(_: TermName) => false
          case _ => true
        }) c.error(c.enclosingPosition, "Invalid do-while test expression!") // should not happen after normalization

        // create body block
        val body = if (b.size > 1) Block(b.slice(0, b.size - 1).toList, b.last) else b.head
        val bodyStartBlock = new CFBlock()
        val bodyEndBlock = _createCFG(body, bodyStartBlock)

        // create condition block with the `cond` term
        val condBlock = new CFBlock()
        condBlock.stats += cond

        // create next block
        val nextBlock = new CFBlock()

        // fix block type
        val loopID = loopCounter.advance.get
        bodyStartBlock.kind = DoWhileBegin(loopID)
        bodyEndBlock.kind = DoWhileEnd(loopID)

        graph += LkDiEdge(currBlock, bodyStartBlock)(true)
        graph += LkDiEdge(bodyEndBlock, condBlock)(true)
        graph += LkDiEdge(condBlock, bodyStartBlock)(true)
        graph += LkDiEdge(condBlock, nextBlock)(false)

        nextBlock

      // while (`cond`) { `body` }
      case LabelDef(_, _, If(cond, Block(b, _), _)) =>
        // ensure that the `cond` tree is a simple identifier
        if (cond match {
          case Ident(_: TermName) => false
          case _ => true
        }) c.error(c.enclosingPosition, "Invalid while test expression!") // should not happen after normalization

        // create condition block with the `cond` term
        val condBlock = new CFBlock()
        condBlock.stats += cond

        // create body block
        val body = if (b.size > 1) Block(b.slice(0, b.size - 1).toList, b.last) else b.head
        val bodyStartBlock = new CFBlock()
        val bodyEndBlock = _createCFG(body, bodyStartBlock)

        // create next block
        val nextBlock = new CFBlock()

        // fix block type
        val loopID = loopCounter.advance.get

        // fix block kinds
        condBlock.kind = WhileBegin(loopID)
        bodyEndBlock.kind = WhileEnd(loopID)

        graph += LkDiEdge(currBlock, condBlock)(true)
        graph += LkDiEdge(condBlock, bodyStartBlock)(true)
        graph += LkDiEdge(condBlock, nextBlock)(false)
        graph += LkDiEdge(bodyEndBlock, condBlock)(true)

        nextBlock

      // if (`cond`) `thenp` else `elsep`
      case If(cond, thenp, elsep) =>
        // ensure that the `cond` tree is a simple identifier
        if (cond match {
          case Ident(_: TermName) => true
          case _ => false
        }) c.error(c.enclosingPosition, "Invalid if-then-else test expression!") // should not happen after normalization

        // create condition block with the `cond` term
        val condBlock = new CFBlock()
        condBlock.stats += cond

        // create thenp block
        val thenpStartBlock = new CFBlock()
        val thenpEndBlock = _createCFG(thenp, thenpStartBlock)

        // create elsep block
        val elsepStartBlock = new CFBlock()
        val elsepEndBlock = _createCFG(elsep, elsepStartBlock)

        // create new current block
        val currNewBlock = new CFBlock()

        // fix block kind
        condBlock.kind = Cond

        graph += LkDiEdge(condBlock, condBlock)(true)
        graph += LkDiEdge(condBlock, thenpStartBlock)(true)
        graph += LkDiEdge(condBlock, elsepStartBlock)(false)
        graph += LkDiEdge(thenpEndBlock, currNewBlock)(true)
        graph += LkDiEdge(elsepEndBlock, currNewBlock)(true)

        currNewBlock

      // default: a term expression
      case _ =>
        currBlock.stats += currTree
        currBlock
    }

    // call recursive helper
    _createCFG(tree, vCurr)

    // remove empty blocks from the end of the graph (might result from tailing if (c) e1 else e2 return statements)
    var emptyBlocks = graph.nodes filter { x => x.diSuccessors.isEmpty && x.stats.isEmpty}
    while (emptyBlocks.nonEmpty) {
      for (block <- emptyBlocks) graph -= block // remove empty block with no successors from the graph
      emptyBlocks = graph.nodes filter { x => x.diSuccessors.isEmpty && x.stats.isEmpty}
    }

    // return the graph
    graph
  }

  /**
   * Normalizes an expression tree.
   *
   * This process includes:
   *
   * - unnesting of compex trees ouside while loop tests.
   * - unnesting of compex trees ouside do-while loop tests.
   * - unnesting of compex trees ouside if-then-else conditions.
   */
  object normalize extends Transformer with (Tree => Tree) {

    val testCounter = new Counter()

    override def transform(tree: Tree): Tree = tree match {
      // while (`cond`) { `body` }
      case LabelDef(_, _, If(cond, Block(body, _), _)) =>
        cond match {
          case Ident(_: TermName) =>
            // if condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // introduce condition variable
            val condVar = TermName(f"testA${testCounter.advance.get}%03d")
            // move the complex test outside the condition
            q"{ var $condVar = $cond; while ($condVar) { $body; $condVar = $cond } }"
        }

      // do { `body` } while (`cond`)
      case LabelDef(_, _, Block(body, If(cond, _, _))) =>
        cond match {
          case Ident(_: TermName) =>
            // if condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // introduce condition variable
            val condVar = TermName(f"testB${testCounter.advance.get}%03d")
            // move the complex test outside the condition
            q"{ var $condVar = null.asInstanceOf[Boolean]; do { $body; $condVar = $cond } while ($condVar) }"
        }

      // if (`cond`) `thenp` else `elsep`
      case If(cond, thenp, elsep) =>
        cond match {
          case Ident(_: TermName) =>
            // if condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // introduce condition value
            val condVal = TermName(f"testC${testCounter.advance.get}%03d")
            // move the complex test outside the condition
            q"val $condVal = $cond; ${If(Ident(condVal), thenp, elsep)}"
        }

      case _ =>
        super.transform(tree)
    }

    def apply(tree: Tree): Tree =
      c.typecheck(q"{ import scala.reflect._; ${transform(untypecheck(tree))} }")
  }

}
