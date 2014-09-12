package eu.stratosphere.emma.macros.program.dataflow

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.util.Counter

import scala.reflect.macros._

trait ComprehensionAnalysis[C <: blackbox.Context] extends ContextHolder[C] {

  import c.universe._
}
