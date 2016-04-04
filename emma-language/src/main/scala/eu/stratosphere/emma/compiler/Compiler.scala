package eu.stratosphere.emma
package compiler

import eu.stratosphere.emma.compiler.ir.core.{Language => CoreLanguage}
import eu.stratosphere.emma.compiler.ir.lnf.{SchemaOptimizations => LNFOptimizations, Language => LNFLanguage}

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends CoreLanguage
  /*           */ with LNFLanguage
  /*           */ with LNFOptimizations {

  /** The underlying universe object. */
  override val universe: Universe
}
