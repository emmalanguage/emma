package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.macros.program.comprehension.TestMacros


object testmacros {

  import scala.language.experimental.macros

  // -----------------------------------------------------
  // test macros
  // -----------------------------------------------------

  final def parallelize[T](e: T): Algorithm[T] = macro TestMacros.parallelize[T]

}
