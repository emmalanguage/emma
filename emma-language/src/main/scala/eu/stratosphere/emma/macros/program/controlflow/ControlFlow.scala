package eu.stratosphere.emma.macros.program.controlflow

import scala.reflect.macros._

private[emma] trait ControlFlow[C <: blackbox.Context]
  extends ControlFlowModel[C]
  with ControlFlowAnalysis[C]
  with ControlFlowCompiler[C] {
}
