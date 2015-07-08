package eu.stratosphere.emma.macros.program

import scala.reflect.macros.blackbox

abstract class ContextHolder[C <: blackbox.Context] {

  val c: blackbox.Context
}
