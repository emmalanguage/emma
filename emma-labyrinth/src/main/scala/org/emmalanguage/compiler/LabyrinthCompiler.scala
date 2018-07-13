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

import com.typesafe.config.Config

import scala.collection.generic.CanBuildFrom
import cats.implicits._


trait LabyrinthCompiler
  extends LabyrinthNormalization
    with LabyrinthLabynization {

  import UniverseImplicits._

  override lazy val implicitTypes: Set[u.Type] = API.implicitTypes ++
    Seq(
      api.Type[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment],
      api.Type[CanBuildFrom[Any,Any,Any]].typeConstructor // Because of https://github.com/emmalanguage/emma/issues/234
    )

  def transformations(implicit cfg: Config): Seq[TreeTransform] = Seq(
    // lifting
    Lib.expand,
    Core.lift,
    // optimizations
    Core.cse iff "emma.compiler.opt.cse" is true,
    Optimizations.foldFusion iff "emma.compiler.opt.fold-fusion" is true,
    // backend
    Comprehension.combine,
    Core.unnest,
    // labyrinth transformations
    labyrinthNormalize,
    labyrinthLabynize,
    prependMemoizeTypeInfoCalls
  ) filterNot (_ == noop)

  // This is partly copy-paste from FlinkCompiler
  /** Prepends `memoizeTypeInfo` calls for `TypeInformation[T]` instances requried in the given `tree`. */
  lazy val prependMemoizeTypeInfoCalls = TreeTransform("LabyrinthCompiler.prependMemoizeTypeInfoCalls", tree => {
    val pref = memoizeTypeInfoCalls(tree)
    tree match {
      case api.Block(stats, expr) => api.Block(pref ++ stats, expr)
      case _ => api.Block(pref, tree)
    }
  })

  /** Generates `Memo.memoizeTypeInfo[T]` calls for all required types `T` in the given `tree`. */
  def memoizeTypeInfoCalls(tree: u.Tree): Seq[u.Tree] = {
    import u.Quasiquote
    for (tpe <- requiredTypeInfos(tree).toSeq.sortBy(_.toString)) yield {
      val ttag = q"implicitly[scala.reflect.runtime.universe.TypeTag[$tpe]]"
      val info = q"org.apache.flink.api.scala.`package`.createTypeInformation[$tpe]"
      q"org.emmalanguage.compiler.Memo.memoizeTypeInfo[$tpe]($ttag, $info)"
    }
  }

  /**
   * Infers types `T` in the given `tree` for which a Flink `TypeInformation[T]` might be required.
   * This is simplified from the version in FlinkCompiler, by getting _all_ the types that appear in the tree. This
   * doesn't introduce too many superflous memoizeTypeInfo calls, because almost everything appears in the job after
   * normalization anyway.
   */
  def requiredTypeInfos(tree: u.Tree): Set[u.Type] =
    api.BottomUp.synthesize({
      case api.TypeQuote(tpe) =>
        Set(tpe.widen)
    }).traverseAny._syn(tree).head
}