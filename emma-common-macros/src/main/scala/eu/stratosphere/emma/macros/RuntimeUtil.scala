package eu.stratosphere.emma.macros

import scala.language.implicitConversions
import scala.reflect.runtime
import scala.tools.reflect.ToolBox

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding substitution,
 * fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait RuntimeUtil extends ReflectUtil {
  val universe: runtime.universe.type = runtime.universe
  val tb: ToolBox[universe.type]
  
  import universe._

  def parse(str: String) =
    tb.parse(str)

  def typeCheck(tree: Tree) =
    if (tree.isType) tb.typecheck(tree, tb.TYPEmode)
    else tb.typecheck(tree)

  /** Syntax sugar for [[Tree]]s. */
  implicit def fromTree(self: Tree): TreeOps =
    new TreeOps(self)
}
