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
import util.Monoids

import cats.instances.all._
import shapeless._

/**
 * Determining which parts of the code might be called from a higher-order context, that is from a UDF of a combinator.
 * Note that some code might be executed both at the driver and in UDFs, which need to be disambiguated, because
 * we will later transform higher-order code differently than first-order code.
 *
 * Note: This code assumes that DataBags don't contain functions. (E.g. it handles a DataBag[Int => Int] incorrectly.)
 */
private[backend] trait Order extends Common {
  self: Core =>

  import API._
  import Core.{Lang => core}
  import UniverseImplicits._

  private[backend] object Order {

    /**
     * If a lambda is given as an argument to one of these methods,
     * then that lambda will be called from higher-order context.
     */
    val combinators = Seq(
      DataBag.ops,
      MutableBag.ops,
      Ops.ops
    ).flatten

    /**
     * Disambiguates order in a tree and gives information on which parts of the code might be executed
     * from a higher-order context.
     *
     * == Preconditions ==
     *
     * - Input must be in ANF.
     * - DataBags don't contain functions. (TODO: Maybe add a check for this in some place like CoreValidate)
     *
     * == Postconditions ==
     *
     * Returns a (disambiguatedTree, highFuns) pair, where
     * disambiguatedTree is an ANF tree where it has been decided for every piece of code whether to treat it
     * as driver-only or high (that is, it can be called from a higher-order context). (Some ValDefs have been
     * duplicated for this.)
     *
     * highFuns contains those TermSymbol which are functions that might be called from higher-order context
     * (see isHighContext and its usage for how to make an inherited attribute from it).
     * Note, that highFuns might contain false positives, i.e. it might contain for code that is only executed
     * in the driver.
     *
     * Algorithm:
     * 1. Collect symbols of all function ValDefs. (funs)
     * 2. Build a graph of funs referencing funs. (funGraph)
     * 3. Collect funs that are given as arguments to combinators. (topLevRefs)
     * 4. Collect funs referenced from top-level. (combRefs)
     * 5. Do graph traversals on funGraph from combRefs and topLevRefs to get all high and
     *    driver-only funs, respectively. (highFuns0, driverFuns)
     * 6. The intersection of highFuns0 and driverFuns are the ambiguous ones. Create new ValDefs
     *    for these, and modify refs to them in high code to the newly created vals.
     */
    lazy val disambiguate: u.Tree => (u.Tree, Set[u.TermSymbol]) = tree => {

      def isFun(sym: u.TermSymbol) =
        api.Sym.funs(sym.info.dealias.widen.typeSymbol)

      // The Set of symbols of ValDefs of functions
      val funs = tree.collect {
        case core.ValDef(sym, _) if isFun(sym) => sym
      }.toSet

      val Attr.all(_, topLevRefs :: combRefs :: _, _, funGraph :: _) =
        api.TopDown
        // Fun Refs
        .synthesize(Attr.collect[Vector, u.TermSymbol]{
          case api.ValRef(sym) if funs contains sym =>
            sym
        })
        // funGraph
        .synthesizeWith[Map[u.TermSymbol, Set[u.TermSymbol]]] {
          case Attr.syn(api.ValDef(sym, rhs), _ :: funRefs :: _) if isFun(sym) =>
            Map(sym -> funRefs.toSet)
        }
        // Am I inside a lambda?
        .inherit {
          case api.Lambda(_,_,_) => true
        }(Monoids.disj)
        // Am I inside a combinator call?
        .inherit {
          case api.DefCall(_, method, _, _) if combinators contains method => true
        }(Monoids.disj)
        // Funs given as arguments to combinators (combRefs)
        .accumulateWith[Vector[u.TermSymbol]] {
          case Attr.inh(api.ValRef(sym), insideCombinator :: _)
            if insideCombinator && (funs contains sym) => Vector(sym)
        }
        // Funs referenced from top-level (topLevRefs)
        .accumulateWith[Vector[u.TermSymbol]] {
          case Attr.inh(api.ValRef(sym), insideCombinator :: insideLambda :: _)
            if !insideLambda && !insideCombinator && (funs contains sym) =>
            Vector(sym)
        }
          .traverseAny(tree)

      // BFS on funGraph, starting from `start`
      def funReachability(start: Vector[u.TermSymbol]): Set[u.TermSymbol] = {
        var reached = start.toSet
        var frontier = start
        while (frontier.nonEmpty) {
          frontier = for {
            u <- frontier
            v <- funGraph(u)
            if !reached(v)
          } yield v
          reached ++= frontier
        }
        reached
      }

      val driverFuns = funReachability(topLevRefs)
      // highFuns0 will also contain the ambiguous ones, which we will soon eliminate
      val highFuns0 = funReachability(combRefs)
      val ambiguousFuns = driverFuns intersect highFuns0
      // Create a map from the ambiguous lambdas to the newly created $high versions
      val ambiguousFunMap = Map((ambiguousFuns map {
        sym => sym -> api.TermSym.free(api.TermName.fresh(sym.name.toString ++ "$high"), sym.typeSignature)
      }).toSeq: _*)
      val newFuns = ambiguousFunMap.values
      val highFuns = highFuns0 -- ambiguousFuns ++ newFuns

      // Note that if a fun is defined inside a high lambda, then it won't be in highFuns,
      // because of the refresh. But this is not a problem, since isHighContext will be inherited to it anyway.
      val isHighContext: u.Tree =?> Boolean = {
        case core.ValDef(sym, _) if highFuns contains sym => true
        case api.DefCall(_, method, _, _) if combinators contains method => true
      }

      val disambiguatedTree = api.TopDown
        .inherit(isHighContext)(Monoids.disj)
        .transformWith {

          case Attr.none(core.Let(vals, defs, expr)) =>
            val newVals = vals flatMap { case v@core.ValDef(lhs, rhs) =>
              if (ambiguousFuns contains lhs) {
                Seq(v, core.ValDef(ambiguousFunMap(lhs), api.Tree.refreshAll(rhs)))
              } else {
                Seq(v)
              }
            }
            core.Let(newVals, defs, expr)

          case Attr.inh(api.ValRef(sym), true :: _) if ambiguousFuns(sym) => api.ValRef(ambiguousFunMap(sym))

        }(tree).tree

      (disambiguatedTree, highFuns)
    }
  }

}
