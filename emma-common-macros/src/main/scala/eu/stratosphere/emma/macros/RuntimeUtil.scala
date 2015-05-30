package eu.stratosphere.emma.macros

import scala.reflect.runtime
import scala.tools.reflect.ToolBox

trait RuntimeUtil extends ReflectUtil {
  val universe: runtime.universe.type = runtime.universe
  val tb: ToolBox[universe.type]
  import universe._

  def parse    (s: String) = tb parse     s
  def typeCheck(t: Tree  ) = tb typecheck t
}
