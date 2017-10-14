/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler

import backend.GenericBackend
import lang.AlphaEq
import lang.core.Core
import lang.source.Source
import lib.Lib
import opt.Optimizations
import tools.GraphTools

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigParseOptions

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends AlphaEq
  with Lib
  with Source
  with Core
  with GenericBackend
  with GraphTools
  with Optimizations {

  import UniverseImplicits._

  /** The underlying universe object. */
  override val u: Universe

  /** Implicit types to be removed */
  lazy val implicitTypes: Set[u.Type] = API.implicitTypes

  lazy val preProcess: Seq[TreeTransform] = Seq(
    Source.removeImplicits(implicitTypes),
    fixSymbolTypes,
    stubTypeTrees,
    unQualifyStatics,
    normalizeStatements,
    Source.normalize
  )

  lazy val postProcess: Seq[TreeTransform] = Seq(
    TreeTransform("api.Owner.atEncl", api.Owner.atEncl),
    qualifyStatics,
    restoreTypeTrees
  )

  def pipeline(
    typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true
  )(
    transformations: TreeTransform*
  ): u.Tree => u.Tree = {

    val bld = Seq.newBuilder[TreeTransform]
    //@formatter:off
    if (typeCheck) bld += TreeTransform("Compiler.typeCheck", (tree: u.Tree) => this.typeCheck(tree))
    if (withPre)   bld ++= preProcess
    bld ++= transformations
    if (withPost)  bld ++= postProcess
    //@formatter:on
    val steps = bld.result()

    if (!printAllTrees) Function.chain(steps.map(_.xfrm))
    else Function.chain(List(print) ++ steps.map(tt => executeAndPrintTransform(tt)))
  }

  implicit class TransformationOps(xfrm: TreeTransform) {
    def iff(k: String)(implicit cfg: Config): Is =
      new Is(k)

    class Is(k: String)(implicit cfg: Config) {
      def is(v: Boolean): TreeTransform =
        if (cfg.getBoolean(k) == v) xfrm
        else noop

      def is(v: String): TreeTransform =
        if (cfg.getString(k) == v) xfrm
        else noop
    }

  }

  val noop = TreeTransform("Predef.identity", Predef.identity[u.Tree] _)

  // Turn this on to print the tree between every step in the pipeline (also before the first and after the last step).
  val printAllTrees = false

  lazy val print: u.Tree => u.Tree = {
    (tree: u.Tree) => {
      println("=============================")
      println(u.showCode(tree))
      println("=============================")
      tree
    }
  }

  def executeAndPrintTransform(tt: TreeTransform): u.Tree => u.Tree = {
    (tree: u.Tree) => {
      val res = tt(tree)
      println()
      println("Transformation: " + tt.name)
      println("=============================")
      println(u.showCode(res))
      println("=============================")
      res
    }
  }

  /** Loads a sequence of resources (in decreasing priority). */
  def loadConfig(paths: Seq[String]): Config = {
    val opts = ConfigParseOptions.defaults().setClassLoader(getClass.getClassLoader)

    val sPrp = ConfigFactory.systemProperties()
    val sEnv = ConfigFactory.systemEnvironment()

    val cfgs = for {
      p <- paths.map(_.stripPrefix("/"))
    } yield Option(getClass.getResource(s"/$p")) match {
      case Some(_) => ConfigFactory.parseResources(p, opts)
      case None => abort(s"Cannot find Emma config resource `/$p`")
    }

    cfgs
      .foldLeft(sEnv withFallback sPrp)((acc, cfg) => acc withFallback cfg)
      .resolve()
  }

  /**
   * Resolves a sequence of config paths to be used with [[loadConfig]]. The result
   *
   * - `path` :+ [[baseConfig]] if `tlPath` is some tree represenging a string literal `path`, or
   * - [[baseConfig]] otherwise.
   *
   * Aborts execution with an error if `tlPath` is not a string literal.
   */
  protected def configPaths(tlPath: Option[u.Tree] = None): Seq[String] =
    tlPath.fold(baseConfig)({
      case api.Lit(path: String) => path +: baseConfig
      case _ => abort("The provided `config` path is not a string literal.")
    })

  def baseConfig = Seq("reference.emma.conf")
}
