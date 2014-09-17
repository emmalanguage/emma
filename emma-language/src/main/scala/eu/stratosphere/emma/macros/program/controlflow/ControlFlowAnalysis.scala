package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.macros.program.util.Counter

import scala.reflect.macros._
import scalax.collection.GraphPredef._
import scalax.collection.edge.LkDiEdge
import scalax.collection.mutable.Graph

private[emma] trait ControlFlowAnalysis[C <: blackbox.Context] extends ControlFlowModel[C] {
  this: ControlFlowModel[C] =>

  import c.universe._

  private type VarStack = List[TermName]

  def emptyVarStack = List[TermName]()

  def createCFG(nCurr: Tree): CFGraph = {

    // loop ID counter
    val loopCounter = new Counter()
    val testCounter = new Counter()

    // vertex ID counter and an implicit next nodeID provider
    val vertexCounter = new Counter()
    implicit def nextVertexID = vertexCounter.advance.get

    // initialize graph
    val vCurr = new CFBlock()
    val graph = Graph[CFBlock, LkDiEdge](vCurr)

    // recursive helper function
    def _createCFG(currTree: Tree, currBlock: CFBlock, X: VarStack): CFBlock = currTree match {
      // { `stats`; `expr` }
      case Block(stats, expr) =>
        var _currBlock = currBlock
        for (s <- stats) {
          _currBlock = _createCFG(s, _currBlock, emptyVarStack)
        }
        _createCFG(expr, _currBlock, X)

      // val `name`: `tpt` = `rhs`
      case ValDef(mods, name, tpt, rhs) =>
        if (containsBranches(rhs)) {
          currBlock.stats += ValDef(Modifiers(Flag.MUTABLE | mods.flags), name, tpt, Literal(Constant(null)))
          _createCFG(rhs, currBlock, name :: X)
        }
        else {
          currBlock.stats += q"$currTree"
          currBlock
        }

      // `name` = `rhs`
      case Assign(Ident(name: TermName), rhs) =>
        if (containsBranches(rhs)) {
          _createCFG(rhs, currBlock, name :: X)
        }
        else {
          currBlock.stats += q"$currTree"
          currBlock
        }

      // do  { `body` } while (`cond`)
      case LabelDef(_, _, Block(b, If(cond, _, _))) =>
        val condStartBlock = new CFBlock()
        val bodyStartBlock = new CFBlock()
        val nextBlock = new CFBlock()

        val body = if (b.size > 1) Block(b.slice(0, b.size - 1).toList, b.last) else b.head

        val currEndBlock = currBlock
        val bodyEndBlock = _createCFG(body, bodyStartBlock, emptyVarStack)

        // fix block type
        val loopID = loopCounter.advance.get
        bodyStartBlock.kind = DoWhileBegin(loopID)
        bodyEndBlock.kind = DoWhileEnd(loopID)

        graph += LkDiEdge(currEndBlock, bodyStartBlock)(true)
        graph += LkDiEdge(bodyEndBlock, bodyStartBlock)(true)
        graph += LkDiEdge(bodyEndBlock, nextBlock)(false)

        nextBlock

      // while (`cond`) { `body` }
      case LabelDef(_, _, If(cond, Block(b, _), _)) =>
        // create condition block
        val condBlock = new CFBlock()
        val (condVar, currEndBlock) = cond match {
          case Ident(condVar: TermName) =>
            (condVar, currBlock)
          case _ =>
            // introduce condition variable and move the complex test outside the condition block
            val condVar = TermName(f"test${testCounter.advance.get}%03d")
            (condVar, _createCFG(q"var $condVar = $cond", currBlock, emptyVarStack))
        }
        condBlock.stats += Ident(condVar)

        // create body block
        val body = if (b.size > 1) Block(b.slice(0, b.size - 1).toList, b.last) else b.head
        val bodyStartBlock = new CFBlock()
        val bodyEndBlock = cond match {
          case Ident(condVar: TermName) =>
            _createCFG(body, bodyStartBlock, emptyVarStack)
          case _ =>
            _createCFG(q"{ $body; $condVar = $cond}", bodyStartBlock, emptyVarStack)
        }

        val nextBlock = new CFBlock()

        // fix block type
        val loopID = loopCounter.advance.get

        condBlock.kind = WhileBegin(loopID)
        bodyEndBlock.kind = WhileEnd(loopID)

        graph += LkDiEdge(currEndBlock, condBlock)(true)
        graph += LkDiEdge(condBlock, bodyStartBlock)(true)
        graph += LkDiEdge(condBlock, nextBlock)(false)
        graph += LkDiEdge(bodyEndBlock, condBlock)(true)

        nextBlock

      // if (`cond`) `thenp` else `elsep`
      case If(cond, thenp, elsep) =>
        // create condition block
        val condBlock = new CFBlock()
        val (condVar, currEndBlock) = cond match {
          case Ident(condVar: TermName) =>
            (condVar, currBlock)
          case _ =>
            // introduce condition variable and move the complex test outside the condition block
            val condVar = TermName(f"test${testCounter.advance.get}%03d")
            (condVar, _createCFG(q"val $condVar = $cond", currBlock, emptyVarStack))
        }
        condBlock.stats += Ident(condVar)

        // create thenp block
        val thenpStartBlock = new CFBlock()
        val thenpEndBlock = _createCFG(thenp, thenpStartBlock, X)

        // create elsep block
        val elsepStartBlock = new CFBlock()
        val elsepEndBlock = _createCFG(elsep, elsepStartBlock, X)

        // create new current block
        val currNewBlock = new CFBlock()

        // fix block type
        condBlock.kind = Cond

        graph += LkDiEdge(currEndBlock, condBlock)(true)
        graph += LkDiEdge(condBlock, thenpStartBlock)(true)
        graph += LkDiEdge(condBlock, elsepStartBlock)(false)
        graph += LkDiEdge(thenpEndBlock, currNewBlock)(true)
        graph += LkDiEdge(elsepEndBlock, currNewBlock)(true)

        currNewBlock

      // default: a term expression
      case _ =>
        if (X.nonEmpty)
          currBlock.stats += Assign(Ident(X.head), q"$currTree")
        else
          currBlock.stats += q"$currTree"

        currBlock
    }

    // call recursive helper
    _createCFG(nCurr, vCurr, emptyVarStack)

    // remove empty blocks from the end of the graph (might result from tailing if (c) e1 else e2 return statements)
    var emptyBlocks = graph.nodes filter { x => x.diSuccessors.isEmpty && x.stats.isEmpty}
    while (emptyBlocks.nonEmpty) {
      for (block <- emptyBlocks) graph -= block // remove empty block with no successors from the graph
      emptyBlocks = graph.nodes filter { x => x.diSuccessors.isEmpty && x.stats.isEmpty}
    }

    // return the graph
    graph
  }

  def analyzeVariables(block: CFBlock) = {
  }

  // FIXME: exclude UDFs in comprehended expressions
  private def containsBranches(tree: Tree): Boolean = tree match {
    case Block(_, expr) =>
      containsBranches(expr)
    case If(_, _, _) =>
      true
    case _ =>
      false
  }
}
