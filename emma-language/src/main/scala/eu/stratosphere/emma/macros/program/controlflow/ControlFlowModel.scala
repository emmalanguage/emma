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

  /**
   * Control flow graph type
   */
  type CFGraph = Graph[CFBlock, LkDiEdge]

  /**
   * Control flow graph: node type
   */
  class CFBlock(implicit val id: Int) {
    // the kind of the block
    var kind: BlockKind = Linear

    // statemetns used in this block
    val stats = mutable.ListBuffer[Tree]()

    // value definitions in this block
    val defs = mutable.ListBuffer[(Name, Int)]()

    // value usages in this block
    val uses = mutable.ListBuffer[(Name, Int)]()

    override def equals(other: Any): Boolean = other match {
      case that: CFBlock => that.id == this.id
      case _ => false
    }

    override def hashCode = id.##

    override def toString = s"CFBlock#$id"

    def defName = TermName(f"block$id%03d")

    def defType = if (stats.nonEmpty && stats.last.tpe != NoType)
      stats.last.tpe match {
        case c.universe.ConstantType(constant) => constant.tpe
        case _ => stats.last.tpe
      }
    else
      c.typeOf[Unit]
  }

  // --------------------------------------------------------------------------
  // Control flow block: node kinds
  // --------------------------------------------------------------------------

  sealed trait BlockKind

  case object Linear extends BlockKind

  case object Cond extends BlockKind

  case class WhileBegin(x: Int) extends BlockKind

  case class WhileEnd(x: Int) extends BlockKind

  case class DoWhileBegin(x: Int) extends BlockKind

  case class DoWhileEnd(x: Int) extends BlockKind

  // --------------------------------------------------------------------------
  // Environment entries
  // --------------------------------------------------------------------------

  /**
   * Data flow graph type
   */
  type DFGraph = Graph[CFBlock, LkDiEdge]

  case class EnvEntry(name: TermName) {

    override def equals(o: Any) = o match {
      case that: EnvEntry => this.name.equals(that.name)
      case _ => false
    }

    override def hashCode = name.hashCode
  }

  // --------------------------------------------------------------------------
  // Auxiliary methods
  // --------------------------------------------------------------------------

  private[program] def findByLabel(source: CFGraph#NodeT, label: Boolean): Option[CFGraph#NodeT] =
    source.outgoing.foldLeft(Option.empty[CFGraph#NodeT])((n, e) => if (e.label == label) Some[CFGraph#NodeT](e._2) else n)
}
