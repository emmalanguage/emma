package eu.stratosphere
package emma.macros.utility

import emma.compiler.MacroCompiler

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class UtilMacros(val c: blackbox.Context) extends MacroCompiler {

  def visualize[T](e: c.Expr[T]) = {
    browse(e.tree)
    e
  }
}
