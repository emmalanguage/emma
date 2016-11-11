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
package compiler.lang.cf

import compiler.Common
import compiler.lang.core.Core
import util.Monoids

import cats.std.all._
import shapeless._

/** Control-flow graph analysis (CFG). */
private[compiler] trait CFG extends Common {
  this: Core with ControlFlow =>

  /** Control-flow graph analysis (CFG). */
  private[compiler] object CFG {

    import UniverseImplicits._
    import Core.{Lang => core}
    import GraphRepresentation.phi

    private val module = Some(core.Ref(GraphRepresentation.module))

    /**
     * A control-flow graph.
     *
     * @tparam V The type of vertices in the graph.
     * @param uses A store for number of references per vertex.
     * @param defs A store for all value definitions (parameters are defined as `phi` nodes).
     * @param flow A directed graph from LHS to free references on the RHS of values.
     * @param nest A directed graph from LHS to nested definitions on the RHS of values.
     */
    case class Graph[V](
        uses: Map[V, Int],
        defs: Map[V, u.ValDef],
        flow: Map[V, Set[V]],
        nest: Map[V, Set[V]]) {

      /** Reverses the direction of `flow` and `nest` from RHS to LHS. */
      def reverse: Graph[V] = Graph(uses, defs, reverse(flow), reverse(nest))

      /** Builds the transitive closure of `flow` and `nest`. */
      def transitive: Graph[V] = Graph(uses, defs, transitive(flow), transitive(nest))

      /** Returns the sub-graph of this graph with vertices satisfying `p`. */
      def filter(p: V => Boolean): Graph[V] = Graph(
        uses.filterKeys(p), defs.filterKeys(p),
        flow.filterKeys(p).mapValues(_.filter(p)),
        nest.filterKeys(p).mapValues(_.filter(p)))

      /** Applies `f` to every vertex in the graph. */
      def map[A](f: V => A): Graph[A] = Graph(
        uses.map { case (k, v) => f(k) -> v },
        defs.map { case (k, v) => f(k) -> v },
        flow.map { case (k, vs) => f(k) -> vs.map(f) },
        nest.map { case (k, vs) => f(k) -> vs.map(f) })

      /** Reverses a map of edges. */
      private def reverse(edges: Map[V, Set[V]]): Map[V, Set[V]] =
        (for ((k, vs) <- edges.toSeq; v <- vs) yield (v, k))
          .groupBy(_._1).mapValues(_.map(_._2).toSet)

      /** Builds the transitive closure of a map of edges. */
      private def transitive(edges: Map[V, Set[V]]): Map[V, Set[V]] = {
        var trans = edges
        var prevSz, currSz = 0
        do {
          trans = for ((k, vs) <- trans)
            yield k -> vs.flatMap(trans.get).fold(vs)(_ union _)
          prevSz = currSz
          currSz = trans.values.map(_.size).sum
        } while (prevSz < currSz)
        trans
      }
    }

    /**
     * Control-flow graph analysis (CFA) of a tree.
     *
     * == Preconditions ==
     * - The input `tree` is in DSCF (see [[DSCF.transform]]).
     *
     * == Postconditions ==
     * - All dataflow dependencies from LHS to RHS are contained in the graph.
     * - All containment relations from LHS to RHS are contained in the graph.
     * - The RHS of method parameters is represented by `phi` nodes.
     */
    def graph(monad: u.ClassSymbol = API.bagSymbol): u.Tree => Graph[u.TermSymbol] = {
      val cs = new Comprehension.Syntax(monad)
      api.TopDown.withBindUses.withBindDefs
        .inherit { // Enclosing comprehension / lambda, if any
          case core.ValDef(lhs, cs.Comprehension(_, _), _) => Option(lhs)
          case core.ValDef(lhs, core.Lambda(_, _, _), _) => Option(lhs)
        } (Monoids.right(None)).inherit { // Accessible methods
          case core.Let(_, defs, _) => defs.map(_.symbol.asTerm).toSet
        }.accumulateWith[Map[u.TermSymbol, Set[u.TermSymbol]]] { // Dataflow edges
          case Attr.syn(core.ValDef(lhs, _, _), defs :: uses :: _) =>
            Map(lhs -> uses.keySet.diff(defs.keySet).filterNot(_.isStatic))
        }.accumulateWith[Map[u.TermSymbol, Set[u.TermSymbol]]] { // Contains edges
          case Attr.inh(core.ValDef(lhs, _, _), _ :: Some(encl) :: _) =>
            Map(encl -> Set(lhs))
        } (Monoids.merge).accumulateWith[Map[u.TermSymbol, Vector[u.Tree]]] { // Phi choices
          case Attr.inh(core.DefCall(None, method, _, argss@_*), local :: _) if local(method) => {
            for ((param, arg) <- method.paramLists.flatten zip argss.flatten)
              yield param.asTerm -> Vector(arg)
          }.toMap
        } (Monoids.merge).traverseAny.andThen {
          case Attr(tree, choices :: nest :: flow :: _, _, defs :: uses :: _) =>
            val dataFlow = flow ++ choices.mapValues(_.iterator.collect {
              case core.Ref(target) if !target.isStatic => target
            }.toSet)

            val values = defs.collect {
              case value @ (api.ValSym(_), _) => value
              case (_, core.ParDef(lhs, _, flags)) if choices.contains(lhs) =>
                val rhs = core.DefCall(module)(phi, lhs.info)(choices(lhs))
                lhs -> core.ParDef(lhs, rhs, flags)
            }

            Graph(uses, values, dataFlow, nest)
        }
    }
  }
}
