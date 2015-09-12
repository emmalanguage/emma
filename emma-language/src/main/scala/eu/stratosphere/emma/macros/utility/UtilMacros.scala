package eu.stratosphere.emma.macros.utility

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class UtilMacros(val c: blackbox.Context) {
  import c.universe._

  def desugar(e: c.Expr[Any]) =
    q"${show(e.tree)}"

  def desugarRaw(e: c.Expr[Any]) =
    q"${showRaw(e.tree)}"
}
