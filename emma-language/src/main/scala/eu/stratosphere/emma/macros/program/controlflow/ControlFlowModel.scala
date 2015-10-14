package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.macros.BlackBox
import scala.collection._
import scala.language.existentials
import scalax.collection.edge.LkDiEdge
import scalax.collection.mutable.Graph

private[emma] trait ControlFlowModel extends BlackBox {
  import universe._

  // --------------------------------------------------------------------------
  // Control flow Graph
  // --------------------------------------------------------------------------

  /** Control flow graph type. */
  type CFGraph = Graph[CFBlock, LkDiEdge]

  /** Control flow graph: node type. */
  class CFBlock(implicit val id: Int) {

    /** The kind of the block. */
    var kind: BlockKind = Linear

    /** Statements used in this block. */
    val stats = mutable.ListBuffer.empty[Tree]

    /** Value definitions in this block. */
    val defs = mutable.ListBuffer.empty[(Name, Int)]

    /** Value usages in this block. */
    val refs = mutable.ListBuffer.empty[(Name, Int)]

    override def equals(other: Any): Boolean = other match {
      case that: CFBlock => id == that.id
      case _ => false
    }

    override def hashCode = id.##

    override def toString = s"CFBlock#$id"

    def defName = TermName(f"block$id%03d")

    def defType = if (stats.nonEmpty && stats.last.tpe != NoType)
      stats.last.tpe match {
        case ConstantType(const) => const.tpe
        case tpe => tpe
      } else typeOf[Unit]
  }

  // --------------------------------------------------------------------------
  // Control flow block: node kinds
  // --------------------------------------------------------------------------

  sealed trait BlockKind
  case object Linear              extends BlockKind
  case object Cond                extends BlockKind
  case class WhileBegin  (i: Int) extends BlockKind
  case class WhileEnd    (i: Int) extends BlockKind
  case class DoWhileBegin(i: Int) extends BlockKind
  case class DoWhileEnd  (i: Int) extends BlockKind

  // --------------------------------------------------------------------------
  // Environment entries
  // --------------------------------------------------------------------------

  /** Data flow graph type. */
  type DFGraph = Graph[CFBlock, LkDiEdge]

  case class EnvEntry(name: TermName) {
    override def equals(other: Any) = other match {
      case that: EnvEntry => name == that.name
      case _ => false
    }

    override def hashCode = name.hashCode
  }

  // --------------------------------------------------------------------------
  // Auxiliary methods
  // --------------------------------------------------------------------------

  private[program] def findByLabel(source: CFGraph#NodeT, label: Boolean): Option[CFGraph#NodeT] =
    source.outgoing.foldLeft(Option.empty[CFGraph#NodeT]) { (node, edge) =>
      if (edge.label == label) Some(edge._2) else node
    }
}
