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
package compiler.lang.core

import compiler.Common

import scala.collection.breakOut

/** Dead code elimination (DCE) for the Core language. */
private[core] trait DCE extends Common {
  self: Core =>

  import Core.{Lang => core}
  import UniverseImplicits._

  private[core] object DCE {

    /**
     * Eliminates unused value definitions (dead code) from a tree.
     *
     * == Preconditions ==
     * - The input tree is in LNF (see [[Core.lnf]]).
     *
     * == Postconditions ==
     * - All unused value definitions are pruned.
     */
    lazy val transform = TreeTransform("DCE.transform",
      api.BottomUp.withDefCalls.withValUses.transformSyn {
        case Attr(let @ core.Let(vals, defs, expr), _, _, syn) =>
          def refs(tree: u.Tree) = syn(tree).head.keySet
          def calls(tree: u.Tree) = syn(tree).tail.head.keySet

          var liveDefs = defs
          var size = 0
          while (size != liveDefs.size) {
            size = liveDefs.size
            liveDefs = liveDefs.filter { d =>
              val m = d.symbol.asMethod
              calls(expr)(m) || liveDefs.exists(calls(_)(m))
            }
          }

          // Gather refs in DefDefs.
          val refsInDefs: Set[u.TermSymbol] = liveDefs.flatMap(refs)(breakOut)

          // Decide for each ValDef whether it is needed.
          // Start with the refs in the expr and refs in the DefDefs, then go through the ValDefs backwards.
          val liveRefs = vals.foldRight(refs(expr) | refsInDefs) {
            // If we already know that the lhs is `live` then we add the refs on the rhs to `live`.
            case (core.ValDef(lhs, rhs), live)
              if live(lhs) =>
              live | refs(rhs)
            // When we have a DefCall that is returning a unit, then treat this val as used
            case (core.ValDef(lhs, rhs @ api.DefCall(_, method, _, _)), live)
              if maybeMutable(method) =>
              live | refs(rhs) + lhs
            // Always treat implicit vals as used
            case (core.ValDef(lhs, rhs), live)
              if lhs.isImplicit =>
              live | refs(rhs) + lhs
            case (_, live) => live
          }

          // Retain only those ValDefs which are referenced.
          val liveVals = vals.filter(liveRefs.compose(_.symbol.asTerm))
          if (liveVals.size == vals.size && liveDefs.size == defs.size) let
          else core.Let(liveVals, liveDefs, expr)
      }.andThen(_.tree))

    private def maybeMutable(method: u.MethodSymbol): Boolean = {
      method.returnType =:= api.Type[Unit]
    } || {
      API.MutableBag.update == method
    }
  }
}
