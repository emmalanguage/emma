package eu.stratosphere.emma
package compiler

import eu.stratosphere.emma.compiler.ir.core.{Language => CoreLanguage}

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends CoreLanguage {

  /** The underlying universe object. */
  override val universe: Universe
}
