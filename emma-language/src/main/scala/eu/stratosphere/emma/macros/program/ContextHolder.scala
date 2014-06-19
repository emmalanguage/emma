package eu.stratosphere.emma.macros.program

import _root_.scala.reflect.macros.blackbox.Context

abstract class ContextHolder[C <: Context] {

  val c: Context
}
