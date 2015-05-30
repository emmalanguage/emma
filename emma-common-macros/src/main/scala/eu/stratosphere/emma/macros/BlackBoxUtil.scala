package eu.stratosphere.emma.macros

import scala.reflect.macros.blackbox

trait BlackBoxUtil extends ReflectUtil {
  val c: blackbox.Context
  val universe: c.universe.type = c.universe
  import universe._

  def parse    (s: String) = c parse     s
  def typeCheck(t: Tree  ) = c typecheck t

  override val freshName = (pre: String) => TermName(c freshName pre)
  override val freshType = (pre: String) => TypeName(c freshName pre)
}
