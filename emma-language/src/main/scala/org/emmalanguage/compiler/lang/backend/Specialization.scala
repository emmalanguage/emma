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
package compiler.lang.backend

import compiler.Common
import compiler.lang.core.Core
import util.Monoids._

import cats.std.all._
import shapeless._

import scala.collection.breakOut

/** Translating to dataflows. */
private[backend] trait Specialization extends Common {
  self: Backend with Core =>

  private[backend] object Specialization {

    import API._
    import UniverseImplicits._
    import Core.{Lang => core}

    private val scalaSeqRef = Some(api.ModuleRef(ScalaSeq$.sym))

    /**
     * Specialize backend calls of order 1 to the given [[BackendAPI]].
     *
     * == Preconditions ==
     *
     * - The input tree is in Core.
     *
     * == Postconditions ==
     *
     * - A tree where API calls of order 1 have been specialized to the given [[BackendAPI]].
     *
     * @param backendAPI The [[BackendAPI]] to use for specialization.
     */
    def specialize(backendAPI: BackendAPI): u.Tree => u.Tree = {

      // object specialization map
      val tgs: Map[u.TermSymbol, u.Tree] = Map(
        //@formatter:off
        API.DataBag$.sym    -> core.Ref(backendAPI.DataBag$.sym),
        API.MutableBag$.sym -> core.Ref(backendAPI.MutableBag$.sym),
        API.Ops.sym         -> core.Ref(backendAPI.Ops.sym)
        //@formatter:on
      )

      // methods specialization map
      val ops: Map[u.MethodSymbol, u.MethodSymbol] =
        Seq(
          //@formatter:off
          API.DataBag$    -> backendAPI.DataBag$,
          API.MutableBag$ -> backendAPI.MutableBag$,
          API.Ops         -> backendAPI.Ops
          //@formatter:on
        ).flatMap({
          case (srcOb, tgtOb) =>
            val srcSorted = srcOb.ops.toSeq.sortBy(_.name.toString)
            val tgtSorted = tgtOb.ops.toSeq.sortBy(_.name.toString)
            srcSorted zip tgtSorted
        })(breakOut)

      Order.disambiguate.andThen { case (disambiguated, highOrd) =>
        val fetched = api.TopDown.break.withBindDefs.withBindUses
          .accumulateWith[Set[u.TermSymbol]] {
            // Build the closure of outermost lambdas wrt to bags used in higher-order context.
            // Descending further is redundant since referenced bags have already been fetched.
            case Attr.syn(core.ValDef(lhs, _), uses :: defs :: _)
              if highOrd(lhs) => for {
                bag <- uses.keySet diff defs.keySet
                if api.Type.constructor(bag.info) =:= DataBag.tpe
              } yield bag
          }.traverse {
            // Break condition same as above.
            case core.ValDef(lhs, _) if highOrd(lhs) =>
          }.andThen[Map[u.TermSymbol, u.ValDef]] {
            case Attr(_, highRef :: _, _, _ :: defs :: _) => highRef.map { lhs =>
              val alias = api.ValSym(lhs.owner, api.TermName.fresh(lhs), lhs.info)
              defs.get(lhs) match {
                // DataBag(Seq(..elements)).fetch() == ScalaSeq(Seq(..elements))
                // Instead of fetching build directly from the underlying Seq.
                case Some(core.ValDef(_, call @ core.DefCall(_, DataBag$.empty | DataBag$.apply, _, _))) =>
                  lhs -> core.ValDef(alias, call)
                case _ =>
                  val targs = Seq(api.Type.arg(1, lhs.info))
                  val argss = Seq(Seq(core.Ref(lhs)))
                  val fetch = core.DefCall(scalaSeqRef, ScalaSeq$.fromDataBag, targs, argss)
                  lhs -> core.ValDef(alias, fetch)
              }
            } (breakOut)
          } (disambiguated)

        api.BottomUp.inherit {
          // Are we nested in a higher-order function?
          case core.ValDef(lhs, _) => highOrd(lhs)
        } (disj).transformWith {
          // Specialize API calls.
          case Attr.inh(core.DefCall(Some(core.Ref(target)), method, targs, argss), false :: _)
            if (tgs contains target) && (ops contains method) =>
            core.DefCall(Some(tgs(target)), ops(method), targs, argss)

          // Insert fetch calls for method parameters (former variables).
          case Attr.none(core.DefDef(method, tps, pss, core.Let(vals, defs, expr)))
            if pss.exists(_.exists(p => fetched.contains(p.symbol.asTerm))) =>
            val paramss = pss.map(_.map(_.symbol.asTerm))
            val body = core.Let(paramss.flatten.collect(fetched) ++ vals, defs, expr)
            core.DefDef(method, tps, paramss, body)

          // Insert fetch calls for values.
          case Attr.none(core.Let(vals, defs, expr))
            if vals.exists(v => fetched.contains(v.symbol.asTerm)) =>
            core.Let(vals.flatMap { case value @ core.ValDef (lhs, _) =>
              fetched.get(lhs).fold(Seq(value))(Seq(value, _))
            }, defs, expr)

          // Replace references in higher-order functions with fetched values.
          case Attr.inh(core.Ref(target), true :: _) if fetched.contains(target) =>
            core.Ref(fetched(target).symbol.asTerm)
        }._tree(disambiguated)
      }
    }
  }
}
