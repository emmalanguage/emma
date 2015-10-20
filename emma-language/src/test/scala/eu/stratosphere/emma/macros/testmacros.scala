package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.macros.program.comprehension.TestMacros

object testmacros {

  import scala.language.experimental.macros

  // -----------------------------------------------------
  // test macros
  // -----------------------------------------------------

  def parallelize[T](e: T): Algorithm[T] = macro TestMacros.parallelize[T]
  def reComprehend[T](e: T): Algorithm[T] = macro TestMacros.reComprehend[T]
}
