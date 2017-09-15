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

import compiler.Common
import compiler.lang.core.Core
import util.Monoids._

import cats.instances.all._
import shapeless._

import scala.collection.breakOut

/** Translating to dataflows. */
private[backend] trait GenericSpecialization extends Common {
  self: Core with Order =>

  private[backend] object GenericSpecialization {

    import API._
    import Core.{Lang => core}
    import UniverseImplicits._

    val foobar = (tree: u.Tree) => {
      Core.validate(Order.disambiguate(tree)._1)
    }

    private val scalaSeq = Some(ScalaSeq$.ref)

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
        API.DataBag$.sym    -> backendAPI.DataBag$.ref,
        API.MutableBag$.sym -> backendAPI.MutableBag$.ref,
        API.Ops.sym         -> backendAPI.Ops.ref
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
        val collected = api.TopDown.break.withBindDefs.withBindUses
          .accumulateWith[Set[u.TermSymbol]] {
            // Build the closure of outermost lambdas wrt to bags used in higher-order context.
            // Descending further is redundant since referenced bags have already been collected.
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
                // DataBag(Seq(..elements)).collect() == ScalaSeq(Seq(..elements))
                // Instead of collecting build directly from the underlying Seq.
                case Some(core.ValDef(_, call @ core.DefCall(_, DataBag$.empty | DataBag$.apply, _, _))) =>
                  lhs -> core.ValDef(alias, call)
                case _ =>
                  val targs = Seq(api.Type.arg(1, lhs.info))
                  val argss = Seq(Seq(core.Ref(lhs)))
                  val dcall = core.DefCall(scalaSeq, ScalaSeq$.fromDataBag, targs, argss)
                  lhs -> core.ValDef(alias, dcall)
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

          // Insert collect calls for method parameters (former variables).
          case Attr.none(core.DefDef(method, tps, pss, core.Let(vals, defs, expr)))
            if pss.exists(_.exists(p => collected.contains(p.symbol.asTerm))) =>
            val paramss = pss.map(_.map(_.symbol.asTerm))
            val body = core.Let(paramss.flatten.collect(collected) ++ vals, defs, expr)
            core.DefDef(method, tps, paramss, body)

          // Insert collect calls for values.
          case Attr.none(core.Let(vals, defs, expr))
            if vals.exists(v => collected.contains(v.symbol.asTerm)) =>
            core.Let(vals.flatMap { case value @ core.ValDef (lhs, _) =>
              collected.get(lhs).fold(Seq(value))(Seq(value, _))
            }, defs, expr)

          // Replace references in higher-order functions with collected values.
          case Attr.inh(core.Ref(target), true :: _) if collected.contains(target) =>
            core.Ref(collected(target).symbol.asTerm)
        }._tree(disambiguated)
      }
    }
  }

}
