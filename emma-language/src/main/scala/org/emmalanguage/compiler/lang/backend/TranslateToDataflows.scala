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

import cats.std.all._
import compiler.Common
import compiler.lang.core.Core
import util.Monoids

import shapeless._

/** Translating to dataflows. */
private[backend] trait TranslateToDataflows extends Common {
  self: Backend with Core =>

  import UniverseImplicits._

  private[backend] object TranslateToDataflows {

    /**
     * Translates into a dataflow on the given backend.
     *
     * == Preconditions ==
     *
     * - The input tree is in Core.
     *
     * == Postconditions ==
     *
     * - A tree where DataBag operations have been translated to dataflow operations.
     *
     * @param backendSymbol The symbol of the target backend.
     */
    def translateToDataflows(backendSymbol: u.ModuleSymbol): u.Tree => u.Tree = tree => {

      // Figure out the order stuff
      val (disambiguatedTree, highFuns): (u.Tree, Set[u.TermSymbol]) = Order.disambiguate(tree)

      val withHighContext = api.TopDown.inherit {
        case Core.Lang.ValDef(sym, _, _) if highFuns contains sym => true
      }(Monoids.disj)

      // Change DataBag sources to the corresponding target backend sources (but only in first-order context)
      val ctorsChanged = withHighContext.transformWith {
        case Attr.inh(Core.Lang.DefCall(Some(api.ModuleRef(API.bagModuleSymbol)), method, targs, argss@_*), false :: _)
          if API.sourceOps(method) => {
          val targetMethodDecl = backendSymbol.info.decl(method.name)
          assert(targetMethodDecl.alternatives.size == 1, s"Target method `$targetMethodDecl` shouldn't be overloaded.")
          val targetMethod = targetMethodDecl.asMethod
          Core.Lang.DefCall(Some(api.ModuleRef(backendSymbol)))(targetMethod, targs: _*)(argss: _*)
        }
      }(disambiguatedTree).tree

      // Gather DataBag vals that are referenced from higher-order context
      val Attr.acc(_, valsRefdFromHigh :: _) = withHighContext.accumulateWith[Set[u.TermSymbol]] {
        case Attr.inh(Core.Lang.ValRef(sym), true :: _)
        if api.Type.of(sym).typeConstructor =:= API.DataBag =>
          Set(sym)
      }.traverseAny(ctorsChanged)

      // Insert fetch calls for the vals in valsRefdFromHigh (but only if they are defined at first-order)
      val collectsInserted0 = withHighContext.transformWith {
        case Attr.inh(api.ValDef(lhs, rhs, flags), false :: _)
        if valsRefdFromHigh(lhs)
        => {
          val origSym = api.TermSym(lhs, api.TermName.fresh("orig"), api.Type.of(lhs)) // will be the original bag
          val fetchCall = Core.Lang.DefCall(Some(api.ModuleRef(API.scalaTraversablModSym))) (
            API.byFetch,
            Core.bagElemTpe(Core.Lang.ValRef(lhs))) (
            Seq(Core.Lang.ValRef(origSym)))
          val fetchedSym = api.TermSym(lhs, api.TermName.fresh("fetched"), api.Type.of(fetchCall))
          val fetchedVal = Core.Lang.ValDef(fetchedSym, fetchCall, flags)
          Core.Lang.ValDef(
            lhs,
            Core.Lang.Let(api.ValDef(origSym, rhs), fetchedVal)()(Core.Lang.ValRef(fetchedSym)),
            flags) // Note: the flags are present also here, and on fetchedVal.
        }
      }(ctorsChanged).tree

      val collectsInserted = Core.flatten(collectsInserted0)

      collectsInserted
    }

  }
}
