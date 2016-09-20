package eu.stratosphere
package emma.macros.utility

import emma.compiler.MacroCompiler

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class UtilMacros(val c: blackbox.Context) extends MacroCompiler {

  val idPipeline: c.Expr[Any] => u.Tree =
    identity(typeCheck = false).compose(_.tree)

  import Core.{Lang => core}

  def visualize[T](e: c.Expr[T]) = {
    browse(e.tree)
    e
  }

  def prettyPrint[T](e: c.Expr[T]): c.Expr[String] = c.Expr(
    core.Lit(
      Core.prettyPrint(idPipeline(e))
    )
  )
}
