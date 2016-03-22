package eu.stratosphere
package emma.macros.utility

import emma.compiler.MacroUtil

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class UtilMacros(val c: blackbox.Context) extends MacroUtil {

  import universe._
  import Tree._

  def desugar(e: Expr[Any]) =
    lit(Tree.show(e.tree))

  def desugarRaw(e: Expr[Any]) =
    lit(showRaw(e.tree))

  def visualize[T](e: Expr[T]) = {
    browse(e.tree)
    e
  }
}
