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

import compiler.backend.FlinkBackend
import compiler.opt.FlinkOptimizations

import cats.implicits._
import com.typesafe.config.Config

trait FlinkCompiler extends Compiler
  with FlinkBackend
  with FlinkOptimizations {

  override val baseConfig = "reference.emma.onFlink.conf" +: super.baseConfig

  override lazy val implicitTypes: Set[u.Type] = API.implicitTypes ++ FlinkAPI.implicitTypes

  import UniverseImplicits._

  /** Prepends `memoizeTypeInfo` calls for `TypeInformation[T]` instances requried in the given `tree`. */
  lazy val prependMemoizeTypeInfoCalls = TreeTransform("FlinkCompiler.prependMemoizeTypeInfoCalls", tree => {
    val pref = memoizeTypeInfoCalls(tree)
    tree match {
      case api.Block(stats, expr) => api.Block(pref ++ stats, expr)
      case _ => api.Block(pref, tree)
    }
  })

  /** Generates `FlinkDataSet.memoizeTypeInfo[T]` calls for all required types `T` in the given `tree`. */
  def memoizeTypeInfoCalls(tree: u.Tree): Seq[u.Tree] = {
    import u.Quasiquote
    for (tpe <- requiredTypeInfos(tree).toSeq.sortBy(_.toString)) yield {
      val ttag = q"implicitly[scala.reflect.runtime.universe.TypeTag[$tpe]]"
      val info = q"org.apache.flink.api.scala.`package`.createTypeInformation[$tpe]"
      q"org.emmalanguage.api.FlinkDataSet.memoizeTypeInfo[$tpe]($ttag, $info)"
    }
  }

  /** Infers types `T` in the given `tree` for which a Flink `TypeInformation[T]` might be required. */
  def requiredTypeInfos(tree: u.Tree): Set[u.Type] =
    api.BottomUp.synthesize({
      case api.DefCall(Some(tgt), m, targs, _)
        if FlinkAPI.DataBag.groupBy.overrides.contains(m) =>
        val K = targs(0)
        val A = api.Type.arg(1, tgt.tpe)
        Set(K, API.Group(K, API.DataBag(A)))
      case api.DefCall(Some(tgt), m, _, _)
        if FlinkAPI.DataBag.zipWithIndex.overrides.contains(m) =>
        val A = api.Type.arg(1, tgt.tpe)
        Set(api.Type.tupleOf(Seq(A, api.Type.long)))
      case api.DefCall(Some(tgt), m, _, _)
        if FlinkAPI.DataBag.sample.overrides.contains(m) =>
        val A = api.Type.arg(1, tgt.tpe)
        val X = api.Type.tupleOf(Seq(A, api.Type.long))
        val Y = api.Type.tupleOf(Seq(api.Type.int, api.Type.arrayOf(api.Type.optionOf(A))))
        Set(X, Y)
      case api.DefCall(_, FlinkAPI.Ops.foldGroup, targs, _) =>
        val B = targs(1)
        val K = targs(2)
        Set(K, API.Group(K, B))
      case api.DefCall(_, FlinkAPI.MutableBag$.apply, targs, _) =>
        val K = targs(0)
        val V = targs(1)
        Set(K, V, api.Type(FlinkAPI.State, Seq(K, V)))
      case api.DefCall(_, FlinkAPI.DataBag$.readText, _, _) =>
        Set(api.Type.string)
      case api.DefCall(_, FlinkAPI.DataBag$.readCSV, targs, _) =>
        val A = targs(0)
        Set(A, api.Type.string)
      case api.DefCall(_, m, _, _)
        if m == FlinkAPI.DataBag.writeCSV.overrides.contains(m) =>
        Set(api.Type.string)
      case api.DefCall(_, FlinkAPI.DataBag$.from, targs, _) =>
        val A = targs(1)
        Set(A)
      case api.DefCall(Some(tgt), FlinkAPI.DataBag.as, _, _) =>
        val A = api.Type.arg(1, tgt.tpe)
        Set(A)
      case api.DefCall(_, m, targs, _)
        if FlinkAPI.GenericOps(m) => targs.toSet
    }).traverseAny._syn(tree).head

  def transformations(implicit cfg: Config): Seq[TreeTransform] = Seq(
    // lifting
    Lib.expand,
    Core.lift,
    // optimizations
    Core.cse iff "emma.compiler.opt.cse" is true,
    FlinkOptimizations.specializeLoops iff "emma.compiler.flink.native-its" is true,
    Optimizations.foldFusion iff "emma.compiler.opt.fold-fusion" is true,
    Optimizations.addCacheCalls iff "emma.compiler.opt.auto-cache" is true,
    // backend
    Comprehension.combine,
    Core.unnest,
    FlinkBackend.transform,
    // lowering
    Core.trampoline iff "emma.compiler.lower" is "trampoline",
    Core.dscfInv iff "emma.compiler.lower" is "dscfInv",
    removeShadowedThis,
    prependMemoizeTypeInfoCalls
  ) filterNot (_ == noop)

  trait NtvAPI extends ModuleAPI {
    //@formatter:off
    val sym               = api.Sym[org.emmalanguage.api.flink.FlinkNtv.type].asModule

    val iterate           = op("iterate")

    val map               = op("map")
    val flatMap           = op("flatMap")
    val filter            = op("filter")

    val broadcast         = op("broadcast")
    val bag               = op("bag")

    override lazy val ops = Set(iterate, map, flatMap, filter, broadcast, bag)
    //@formatter:on
  }

  object FlinkAPI extends BackendAPI {
    lazy val RuntimeContext = api.Type[org.apache.flink.api.common.functions.RuntimeContext]
    lazy val TypeInformation = api.Type[org.apache.flink.api.common.typeinfo.TypeInformation[Any]].typeConstructor
    lazy val ExecutionEnvironment = api.Type[org.apache.flink.api.scala.ExecutionEnvironment]

    lazy val implicitTypes = Set(TypeInformation, ExecutionEnvironment)

    lazy val State = api.Type[org.emmalanguage.api.FlinkMutableBag.State[Any, Any]].typeConstructor

    lazy val DataBag = new DataBagAPI(api.Sym[org.emmalanguage.api.FlinkDataSet[Any]].asClass)

    lazy val DataBag$ = new DataBag$API(api.Sym[org.emmalanguage.api.FlinkDataSet.type].asModule)

    lazy val MutableBag = new MutableBagAPI(api.Sym[org.emmalanguage.api.FlinkMutableBag[Any, Any]].asClass)

    lazy val MutableBag$ = new MutableBag$API(api.Sym[org.emmalanguage.api.FlinkMutableBag.type].asModule)

    lazy val Ops = new OpsAPI(api.Sym[org.emmalanguage.api.flink.FlinkOps.type].asModule)

    lazy val Ntv = new NtvAPI {}

    lazy val GenericOps = for {
      ops <- Set(DataBag.ops, DataBag$.ops, MutableBag.ops, MutableBag$.ops, Ops.ops)
      sym <- ops
      if sym.info.takesTypeArgs
      res <- sym +: sym.overrides
    } yield res
  }

}
