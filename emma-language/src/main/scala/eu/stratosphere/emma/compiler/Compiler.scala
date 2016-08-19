package eu.stratosphere.emma
package compiler

import lang.AlphaEq
import lang.core.Core
import lang.source.Source

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends AlphaEq with Source with Core {

  /** The underlying universe object. */
  override val universe: Universe

  /** Brings a tree into form convenient for transformation. */
  lazy val preProcess: u.Tree => u.Tree =
    fixLambdaTypes
      .andThen(unQualifyStaticModules)
      .andThen(normalizeStatements)
      .andThen(Source.normalize)

  /** Brings a tree into a form acceptable for `scalac` after being transformed. */
  lazy val postProcess: u.Tree => u.Tree =
    qualifyStaticModules
      .andThen(api.Owner.at(get.enclosingOwner))

  /** The identity transformation with pre- and post-processing. */
  def identity(typeCheck: Boolean = false): u.Tree => u.Tree =
    pipeline(typeCheck)()

  /** Combines a sequence of `transformations` into a pipeline with pre- and post-processing. */
  def pipeline(typeCheck: Boolean = false)
    (transformations: (u.Tree => u.Tree)*): u.Tree => u.Tree = {

    val init = if (typeCheck) preProcess.compose[u.Tree](Type.check(_)) else preProcess
    transformations.foldLeft(init) { _ andThen _ } andThen postProcess
  }
}
