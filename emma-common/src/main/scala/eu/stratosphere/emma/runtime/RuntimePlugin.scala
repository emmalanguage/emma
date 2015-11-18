package eu.stratosphere.emma.runtime

import eu.stratosphere.emma.ir.Combinator

abstract class RuntimePlugin {
  def handleLogicalPlan(root:Combinator[_], name: String, closure: Any*)
}