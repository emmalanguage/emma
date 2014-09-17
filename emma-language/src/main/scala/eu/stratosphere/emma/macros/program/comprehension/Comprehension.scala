package eu.stratosphere.emma.macros.program.comprehension

import scala.reflect.macros._

private[emma] trait Comprehension[C <: blackbox.Context]
  extends ComprehensionModel[C]
  with ComprehensionAnalysis[C]
  with ComprehensionCompiler[C] {
}
