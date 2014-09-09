package eu.stratosphere.emma.macros.program

import scala.reflect.macros.blackbox.Context

abstract class ContextHolder[C <: Context] {

  val c: Context
}
