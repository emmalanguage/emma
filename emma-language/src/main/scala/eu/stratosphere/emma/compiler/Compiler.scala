package eu.stratosphere.emma
package compiler

import lang.AlphaEq
import lang.core.Core
import lang.source.Source
import lang.backend.Backend

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends AlphaEq with Source with Core with Backend {

  /** The underlying universe object. */
  override val universe: Universe

  /** Standard pipeline prefix. Brings a tree into form convenient for transformation. */
  lazy val preProcess: Seq[u.Tree => u.Tree] = Seq(
    fixLambdaTypes,
    unQualifyStaticModules,
    normalizeStatements,
    Source.normalize,
    resolveNameClashes
  )

  /** Standard pipelien suffix. Brings a tree into a form acceptable for `scalac` after being transformed. */
  lazy val postProcess: Seq[u.Tree => u.Tree] = Seq(
    qualifyStaticModules,
    api.Owner.at(get.enclosingOwner)
  )

  /** The identity transformation with pre- and post-processing. */
  def identity(typeCheck: Boolean = false): u.Tree => u.Tree =
    pipeline(typeCheck)()

  /** Combines a sequence of `transformations` into a pipeline with pre- and post-processing. */
  def pipeline(typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true)
    (transformations: (u.Tree => u.Tree)*): u.Tree => u.Tree = {

    val bld = Seq.newBuilder[u.Tree => u.Tree]
    //@formatter:off
    if (typeCheck) bld += { api.Type.check(_) }
    if (withPre)   bld ++= preProcess
    bld ++= transformations
    if (withPost)  bld ++= postProcess
    //@formatter:on
    Function.chain(bld.result())
  }
}
