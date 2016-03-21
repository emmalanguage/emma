package eu.stratosphere.emma
package compiler

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.reflect.runtime.JavaUniverse
import scala.tools.reflect.ToolBox

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding
 * substitution, fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait RuntimeUtil extends ReflectUtil {

  val universe: JavaUniverse = new JavaUniverse
  val tb: ToolBox[universe.type]

  import universe._
  import internal._

  private val logger =
    Logger(LoggerFactory.getLogger(classOf[RuntimeUtil]))

  override def warning(pos: Position, msg: String): Unit =
    logger.warn(s"warning at position $pos: $msg")

  override def abort(pos: Position, msg: String): Nothing =
    throw new RuntimeException(s"error at position $pos: $msg")

  override def parse(code: String): Tree =
    tb.parse(code)

  override def typeCheck(tree: Tree, typeMode: Boolean = false): Tree =
    if (typeMode) tb.typecheck(tree, tb.TYPEmode)
    else tb.typecheck(tree)

  override def termSymbol(
    owner: Symbol,
    name: TermName,
    flags: FlagSet,
    pos: Position): TermSymbol = {

    newTermSymbol(owner, name, pos, flags)
  }

  override def typeSymbol(
    owner: Symbol,
    name: TypeName,
    flags: FlagSet,
    pos: Position): TypeSymbol = {

    newTypeSymbol(owner, name, pos, flags)
  }
}
