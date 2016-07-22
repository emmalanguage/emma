package eu.stratosphere.emma
package compiler

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.reflect.macros.Attachments
import scala.reflect.runtime.JavaUniverse
import scala.tools.reflect.ToolBox

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding
 * substitution, fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait RuntimeUtil extends ReflectUtil {

  val universe: JavaUniverse = scala.reflect.runtime.universe.asInstanceOf[JavaUniverse]
  val tb: ToolBox[universe.type]
  import universe._

  private val logger =
    Logger(LoggerFactory.getLogger(classOf[RuntimeUtil]))

  override def warning(pos: Position, msg: String): Unit =
    logger.warn(s"warning at position $pos: $msg")

  override def abort(pos: Position, msg: String): Nothing =
    throw new RuntimeException(s"error at position $pos: $msg")

  // ------------------------
  // Parsing and typechecking
  // ------------------------

  private[compiler] override def parse(code: String): Tree =
    tb.parse(code)

  private[compiler] override def typeCheck(tree: Tree, typeMode: Boolean = false): Tree =
    if (typeMode) tb.typecheck(tree, tb.TYPEmode)
    else tb.typecheck(tree)

  // ------------------------
  // Abstract wrapper methods
  // ------------------------

  private[compiler] override lazy val enclosingOwner =
    Type.check(q"val x = 42").symbol.owner

  private[compiler] override def getFlags(sym: Symbol): FlagSet =
    internal.flags(sym)

  private[compiler] override def setFlags(sym: Symbol, flags: FlagSet): Unit =
    internal.setFlag(sym, flags)

  private[compiler] def resetFlags(sym: Symbol, flags: FlagSet): Unit =
    internal.resetFlag(sym, flags)

  private[compiler] override def setName(sym: Symbol, name: Name): Unit =
    sym.name = name

  private[compiler] override def setOwner(sym: Symbol, owner: Symbol): Unit =
    sym.owner = owner

  private[compiler] override def setPos(tree: Tree, pos: Position): Unit =
    tree.pos = pos

  private[compiler] override def setOriginal(tt: TypeTree, original: Tree): Unit =
    tt.setOriginal(original)

  private[compiler] override def annotate(sym: Symbol, ans: Annotation*): Unit =
    sym.setAnnotations(ans.toList)

  private[compiler] override def attachments(sym: Symbol): Attachments =
    sym.attachments

  private[compiler] override def attachments(tree: Tree): Attachments =
    tree.attachments

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
