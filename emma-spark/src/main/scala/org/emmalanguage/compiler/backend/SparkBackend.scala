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
package compiler.backend

import compiler.Compiler
import compiler.SparkCompiler

import scala.collection.breakOut

/** Translating to dataflows. */
private[compiler] trait SparkBackend extends Compiler {
  self: SparkCompiler =>

  object SparkBackend {

    import Core.{Lang => core}
    import SparkAPI._
    import UniverseImplicits._

    /* Target objects specialization map. */
    private val T = Map(
      //@formatter:off
      API.DataBag$.sym    -> DataBag$.ref,
      API.MutableBag$.sym -> MutableBag$.ref,
      API.Ops.sym         -> Ops.ref
      //@formatter:on
    ): Map[u.TermSymbol, u.Tree]

    /* Methods specialization map. */
    private val M = (for {
      (srcAPI, tgtAPI) <- Seq(
        //@formatter:off
        API.DataBag$    -> DataBag$,
        API.MutableBag$ -> MutableBag$,
        API.Ops         -> Ops
        //@formatter:on
      )
      (srcOp, tgtOp) <- {
        val srcOps = srcAPI.ops.toSeq.sortBy(_.name.toString)
        val tgtOps = tgtAPI.ops.toSeq.sortBy(_.name.toString)
        srcOps zip tgtOps
      }
    } yield srcOp -> tgtOp) (breakOut): Map[u.MethodSymbol, u.MethodSymbol]

    /** Specialize backend for Spark. */
    val transform = TreeTransform("SparkBackend.transform", tree => {
      val G = ControlFlow.cfg(tree)
      val C = Context.bCtxGraph(G)

      // collect def -> use edges of DataBag terms that need to be broadcast
      // each $x -> $y edge is labeled with two ValDef trees:
      // - val $u = SparkNtv.broadcast($x)
      // - val $v = SparkNtv.bag($u)
      val broadcasts = for {
        x <- C.contexts
        if x.label == BCtx.Driver
        if x.vertex.info.typeSymbol == API.DataBag.sym
        uses = x.successors.filter(C.label(_).contains(BCtx.Engine))
        if uses.nonEmpty
        u = {
          val A = api.Type.arg(1, x.vertex.info)

          val tpe = api.Type(Ntv.Broadcast, Seq(A))
          val nme = api.TermName.fresh(x.vertex.name)
          val lhs = api.TermSym(x.vertex.owner, nme, tpe)

          val tgt = Some(Ntv.ref)
          val met = Ntv.broadcast
          val tas = Seq(A)
          val ass = Seq(Seq(core.Ref(x.vertex)))
          val rhs = api.DefCall(tgt, met, tas, ass)

          core.ValDef(lhs, rhs)
        }
        y <- uses
        v = {
          val A = api.Type.arg(1, x.vertex.info)

          val tpe = API.DataBag(A)
          val nme = api.TermName.fresh(x.vertex.name)
          val lhs = api.TermSym(y.owner, nme, tpe)

          val tgt = Some(Ntv.ref)
          val met = Ntv.bag
          val tas = Seq(A)
          val ass = Seq(Seq(core.Ref(u.symbol.asTerm)))
          val rhs = api.DefCall(tgt, met, tas, ass)

          core.ValDef(lhs, rhs)
        }
      } yield quiver.LEdge(x.vertex, y, (u, v))
      // lookup map: $x -> $u
      val bcDef = broadcasts.map(
        x => x.from -> x.label._1
      )(breakOut): Map[u.TermSymbol, u.ValDef]
      // lookup map: $y -> $v*
      val bcUse = broadcasts.map(
        x => x.to -> x.label._2
      ).groupBy(_._1).mapValues(_.map(_._2)).withDefault(_ => Seq.empty)
      // lookup map: $y -> ($x -> $v)*
      val bcMap = broadcasts.map(
        x => x.to -> (x.from -> x.label._2.symbol.asTerm)
      ).groupBy(_._1).mapValues(_.map(_._2))

      api.BottomUp.transform({
        // specialize API calls
        case core.ValDef(lhs, core.DefCall(Some(core.Ref(t)), m, targs, argss))
          if C.label(lhs).contains(BCtx.Driver) && (T contains t) && (M contains m) =>
          core.ValDef(lhs, core.DefCall(Some(T(t)), M(m), targs, argss))

        // append `SparkNtv.broadcast` calls for `bcDef` method parameters (former variables)
        case core.DefDef(method, tps, pss, core.Let(vals, defs, expr)) =>
          val paramss = pss.map(_.map(_.symbol.asTerm))
          val body = core.Let(paramss.flatten.collect(bcDef) ++ vals, defs, expr)
          core.DefDef(method, tps, paramss, body)

        // append `SparkNtv.broadcast` calls for `bcDef` values
        case core.Let(vals, defs, expr)
          if vals.exists(bcDef contains _.symbol.asTerm) =>
          core.Let(vals flatMap { case value@core.ValDef(lhs, _) =>
            bcDef.get(lhs).fold(Seq(value))(Seq(value, _))
          }, defs, expr)

        // prepend `SparkNtv.bag` calls for `bcUse` values
        case core.Let(vals, defs, expr)
          if vals.exists(bcUse contains _.symbol.asTerm) =>
          core.Let(vals flatMap { case value@core.ValDef(lhs, _) =>
            val pref = bcUse(lhs)
            if (pref.isEmpty) Seq(value)
            else pref :+ api.Tree.rename(bcMap(lhs))(value).asInstanceOf[u.ValDef]
          }, defs, expr)
      })._tree(tree)
    })
  }

}
