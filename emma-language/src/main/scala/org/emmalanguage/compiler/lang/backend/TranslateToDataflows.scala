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
  import Core.{Lang => core}
  
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
        case core.ValDef(sym, _) if highFuns contains sym => true
      }(Monoids.disj)

      // Change static method calls to their corresponding backend calls.
      // The backend methods should be defined in the module `backendSymbol`,
      // and they should not have any overloads (this is because we look up the corresponding method
      // just by name; adding overload resolution would be possible but complicated because of the additional
      // implicit parameters for the backend contexts).
      val moduleSymbols = Set(API.bagModuleSymbol, ComprehensionCombinators.module)
      val methods = API.sourceOps ++ ComprehensionCombinators.ops
      val staticCallsChanged = withHighContext.transformWith {
        case Attr.inh(core.DefCall(Some(api.ModuleRef(moduleSymbol)), method, targs, argss), false :: _)
          if moduleSymbols(moduleSymbol) && methods(method) =>
          val targetMethodDecl = backendSymbol.info.decl(method.name)
          assert(targetMethodDecl.alternatives.size == 1,
            s"Target method `${method.name}` (found as `$targetMethodDecl`) should have exactly one overload.")
          val targetMethod = targetMethodDecl.asMethod
          core.DefCall(Some(api.ModuleRef(backendSymbol)), targetMethod, targs, argss)
      }(disambiguatedTree).tree

      // Gather DataBag vals that are referenced from higher-order context
      val Attr.acc(_, valsRefdFromHigh :: _) =
        withHighContext.accumulateWith[Set[u.TermSymbol]] {
          case Attr.inh(core.ValRef(sym), true :: _)
          if api.Type.constructor(sym.info) =:= API.DataBag =>
            Set(sym)
        }.traverseAny(staticCallsChanged)

      // Insert fetch calls for the vals in valsRefdFromHigh (but only if they are defined at first-order)
      val collectsInserted0 =
        withHighContext.transformWith {
          case Attr.inh(api.ValDef(lhs, rhs), false :: _)
          if valsRefdFromHigh(lhs) =>
            // will be the original bag
            val origSym = api.TermSym(lhs, api.TermName.fresh("orig"), lhs.info)
            val fetchCall = core.DefCall(Some(api.ModuleRef(API.scalaSeqModuleSymbol)),
              API.byFetch,
              Seq(Core.bagElemTpe(core.ValRef(lhs))),
              Seq(Seq(core.ValRef(origSym))))
            val fetchedSym = api.TermSym(lhs, api.TermName.fresh("fetched"), fetchCall.tpe)
            val fetchedVal = core.ValDef(fetchedSym, fetchCall)
            core.ValDef(
              lhs,
              core.Let(Seq(api.ValDef(origSym, rhs), fetchedVal), Seq.empty, core.ValRef(fetchedSym)))
          // Note: the flags are present also here, and on fetchedVal.
        }(staticCallsChanged).tree

      Core.flatten(collectsInserted0) // This is because we put a Let on the rhs of a ValDef when inserting collects
    }

  }
}
