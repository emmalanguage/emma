package eu.stratosphere.emma
package compiler

import eu.stratosphere.emma.compiler.lang.source.Source
import eu.stratosphere.emma.compiler.lang.{AlphaEq, PrettyPrint}
import eu.stratosphere.emma.compiler.lang.core.Core

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends AlphaEq with Source with Core with PrettyPrint {

  /** The underlying universe object. */
  override val universe: Universe

  import universe._

  lazy val unqualifyStaticSels: Tree => Tree = transform {
    case sel: Select if sel.symbol.isStatic =>
      Ident(sel.symbol)
  }

  lazy val qualifyStaticRefs: Tree => Tree = transform {
    case id: Ident if id.symbol.isStatic =>
      Tree.resolve(id.symbol)
  }

}
