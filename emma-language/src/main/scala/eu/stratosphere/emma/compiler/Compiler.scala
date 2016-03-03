package eu.stratosphere.emma
package compiler

import eu.stratosphere.emma.compiler.ir.core.{Language => CoreLanguage}
import eu.stratosphere.emma.compiler.ir.lnf.{Language => LNFLanguage}

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends CoreLanguage with LNFLanguage {

  /** The underlying universe object. */
  override val universe: Universe
}
