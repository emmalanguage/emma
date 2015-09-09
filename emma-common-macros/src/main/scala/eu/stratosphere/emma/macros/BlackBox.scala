package eu.stratosphere.emma.macros

import scala.reflect.macros.blackbox

/**
 * Trait to mix in with black box macros. Once a [[blackbox.Context]] has been provided, its
 * universe will be available in the body of extending classes.
 */
trait BlackBox {
  val c: blackbox.Context
  val universe: c.universe.type = c.universe
}
