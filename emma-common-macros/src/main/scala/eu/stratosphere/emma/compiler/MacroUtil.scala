package eu.stratosphere.emma
package compiler

import scala.reflect.macros.Attachments
import scala.reflect.macros.blackbox
import scala.tools.nsc.Global

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding
 * substitution, fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait MacroUtil extends ReflectUtil {

  val c: blackbox.Context
  val universe: c.universe.type = c.universe
  import universe._

  /** Shows `tree` in a Swing AST browser. */
  def browse(tree: Tree): Unit = universe match {
    case global: Global =>
      val gt = tree.asInstanceOf[global.Tree]
      import global.treeBrowsers._
      val frame = new BrowserFrame("macro-expand")
      val lock = new concurrent.Lock
      frame.setTreeModel(new ASTTreeModel(gt))
      frame.createFrame(lock)
      lock.acquire()
    case _ =>
  }

  override def warning(pos: Position, msg: String): Unit =
    c.warning(pos, msg)

  override def abort(pos: Position, msg: String): Nothing =
    c.abort(pos, msg)

  // ------------------------
  // Parsing and typechecking
  // ------------------------

  private[compiler] override def parse(code: String): Tree =
    c.parse(code)

  private[compiler] override def typeCheck(tree: Tree, typeMode: Boolean = false): Tree =
    if (typeMode) c.typecheck(tree, c.TYPEmode)
    else c.typecheck(tree)

  // ------------------------
  // Abstract wrapper methods
  // ------------------------

  private[compiler] override def enclosingOwner: Symbol =
    c.internal.enclosingOwner

  private[compiler] override def getFlags(sym: Symbol): FlagSet =
    internal.flags(sym)

  private[compiler] override def setFlags(sym: Symbol, flags: FlagSet): Unit =
    internal.setFlag(sym, flags)

  private[compiler] def resetFlags(sym: Symbol, flags: FlagSet): Unit =
    internal.resetFlag(sym, flags)

  private[compiler] override def setName(sym: Symbol, name: Name): Unit =
    internal.setName(sym, name)

  private[compiler] override def setOwner(sym: Symbol, owner: Symbol): Unit =
    internal.setOwner(sym, owner)

  private[compiler] override def setPos(tree: Tree, pos: Position): Unit =
    internal.setPos(tree, pos)

  private[compiler] override def setOriginal(tt: TypeTree, original: Tree): Unit =
    internal.setOriginal(tt, original)

  private[compiler] override def annotate(sym: Symbol, ans: Annotation*): Unit =
    internal.setAnnotations(sym, ans: _*)

  private[compiler] override def attachments(sym: Symbol): Attachments =
    internal.attachments(sym)

  private[compiler] override def attachments(tree: Tree): Attachments =
    internal.attachments(tree)

  private[compiler] override def termSymbol(
    owner: Symbol,
    name: TermName,
    flags: FlagSet,
    pos: Position): TermSymbol = {

    internal.newTermSymbol(owner, name, pos, flags)
  }

  private[compiler] override def typeSymbol(
    owner: Symbol,
    name: TypeName,
    flags: FlagSet,
    pos: Position): TypeSymbol = {

    internal.newTypeSymbol(owner, name, pos, flags)
  }
}
