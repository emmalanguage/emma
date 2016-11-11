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

  import UniverseImplicits._
  import Core.{Lang => core}

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
    lazy val transform: u.Tree => u.Tree =
      api.BottomUp.withValUses.transformWithSyn {
        case Attr(let @ core.Let(vals, defs, expr), _, _, syn) =>
          def refs(tree: u.Tree) = syn(tree).head.keySet

          // Gather refs in DefDefs.
          val refsInDefs: Set[u.TermSymbol] = defs.flatMap(refs)(breakOut)

          // Decide for each ValDef whether it is needed.
          // Start with the refs in the expr and refs in the DefDefs, then go through the ValDefs backwards.
          val liveRefs = vals.foldRight(refs(expr) | refsInDefs) {

            // When we have a DefCall that is returning a unit, then treat this val as used
            case (core.ValDef(lhs, rhs@api.DefCall(_, method, _, _*), _), live)
              if method.returnType =:= api.Type[Unit] =>
              live | refs(rhs) + lhs

            case (core.ValDef(lhs, rhs, _), live) =>
              if (live(lhs)) live | refs(rhs) else live
          }

          // Retain only those ValDefs which are referenced.
          val liveVals = vals.filter(liveRefs.compose(_.symbol.asTerm))
          if (liveVals.size == vals.size) let
          else core.Let(liveVals: _*)(defs: _*)(expr)
      }.andThen(_.tree)
  }
}
