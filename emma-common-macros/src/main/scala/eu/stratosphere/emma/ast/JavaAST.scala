package eu.stratosphere
package emma.ast

import emma.compiler.RuntimeUtil

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.reflect.macros.Attachments
import scala.reflect.runtime
import scala.reflect.runtime.JavaUniverse
import scala.tools.reflect.ToolBox

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding
 * substitution, fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait JavaAST extends RuntimeUtil with AST {

  require(runtime.universe.isInstanceOf[JavaUniverse],
    s"Unsupported universe ${runtime.universe}.\nThe runtime compiler supports only JVM.")

  override val universe = runtime.universe.asInstanceOf[JavaUniverse]
  val tb: ToolBox[universe.type]

  import universe._
  import internal._

  private val logger =
    Logger(LoggerFactory.getLogger(classOf[JavaAST]))

  override def abort(msg: String, pos: Position = NoPosition): Nothing =
    throw new RuntimeException(s"Error at $pos: $msg")

  override def warning(msg: String, pos: Position = NoPosition): Unit =
    logger.warn(s"Warning at $pos: $msg")

  // ---------------------------
  // Parsing and type-checking
  // ---------------------------

  private[emma] override def parse(code: String): Tree =
    tb.parse(code)

  private[emma] override def typeCheck(tree: Tree, typeMode: Boolean = false): Tree =
    if (typeMode) tb.typecheck(tree, tb.TYPEmode) else tb.typecheck(tree)

  // ------------------------
  // Abstract wrapper methods
  // ------------------------

  override val get: Getter = new Getter {
    override lazy val enclosingOwner: Symbol = typeCheck(q"val x = ()").symbol.owner
    override def meta(sym: Symbol): Attachments = attachments(sym)
    override def meta(tree: Tree): Attachments = attachments(tree)
  }

  private[ast] override val set: Setter = new Setter {
    override def name(sym: Symbol, name: Name): Unit = sym.name = name
    override def original(tpt: TypeTree, original: Tree): Unit = tpt.setOriginal(original)
    override def owner(sym: Symbol, owner: Symbol): Unit = sym.owner = owner
    override def pos(sym: Symbol, pos: Position): Unit = sym.pos = pos
    override def pos(tree: Tree, pos: Position): Unit = tree.pos = pos
    override def flags(sym: Symbol, flags: FlagSet): Unit = {
      sym.resetFlags()
      setFlag(sym, flags)
    }
  }
}
