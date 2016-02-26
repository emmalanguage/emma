package eu.stratosphere.emma.macros

import scala.reflect.macros.blackbox

/**
 * Trait to mix in with black box macros. Once a [[blackbox.Context]] has been provided, its
 * universe will be available in the body of extending classes.
 */
@deprecated("Use `emma.compiler.BlackBoxUtil` instead", "24.02.2016")
trait BlackBox {
  val c: blackbox.Context
  val universe: c.universe.type = c.universe
}
