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
import compiler.ir.DSCFAnnotations.continuation
import compiler.lang.core.Core
import util.Monoids._

import cats.instances.all._
import quiver._
import shapeless._

import scala.collection.breakOut

/** Control-flow graph (CFG) construction. */
private[compiler] trait ControlFlow extends Common {
  self: Core =>

  case class FlowGraph[V]
  (
    uses: Map[V, Int],
    nest: Graph[V, Unit, Unit],
    ctrl: Graph[V, u.DefDef, Unit],
    data: Graph[V, u.ValDef, Unit]
  )

  /** Control-flow graph analysis (CFG). */
  object ControlFlow {

    import API.GraphRepresentation.phi
    import Core.{Lang => core}
    import UniverseImplicits._

    private val module = Some(API.GraphRepresentation.ref)

    /**
     * Control-flow graph analysis (CFA) of a tree.
     *
     * == Preconditions ==
     * - The input `tree` is in DSCF (see [[DSCF.transform]]).
     *
     * == Postconditions ==
     * - The RHS of method parameters is represented by `phi` nodes.
     * - The graph contains:
     *   - The number of references for each val / parameter;
     *   - All nest(ing) relations from outer to inner;
     *   - All control (flow) dependencies from caller to callee;
     *   - All data (flow) dependencies from RHS to LHS.
     */
    lazy val cfg: u.Tree => FlowGraph[u.TermSymbol] = {
      api.TopDown.withDefDefs.withBindUses.withBindDefs
        .inherit { case core.TermDef(encl) => Option(encl) } (last(None))
        .accumulateWith[Vector[UEdge[u.TermSymbol]]] { // Nesting
          case Attr.inh(core.BindingDef(lhs, _), Some(encl) :: _) =>
            Vector(LEdge(encl, lhs, ()))
          case Attr.inh(core.DefDef(method, _, _, _), Some(encl) :: _) =>
            Vector(LEdge(encl, method, ()))
        }.accumulate { // Control flow
          case core.DefDef(f, _, _, core.Let(_, _, expr)) =>
            expr.collect { case core.DefCall(None, g, _, _)
              if api.Sym.findAnn[continuation](g).isDefined =>
              LEdge(f.asTerm, g.asTerm, ())
            }.toVector
        }.accumulateWith[Vector[UEdge[u.TermSymbol]]] { // Data flow
          case Attr.inh(core.BindingRef(from), Some(to) :: _) if !from.isStatic =>
            Vector(LEdge(from, to, ()))
        }.accumulate { // Phi choices
          case core.DefCall(None, method, _, argss)
            if api.Sym.findAnn[continuation](method).isDefined => {
              for ((param, arg) <- method.paramLists.flatten zip argss.flatten)
                yield param.asTerm -> Vector(arg)
            }.toMap
        } (merge).traverseAny.andThen {
          case Attr(_, choices :: data :: ctrl :: nest :: _, _, vals :: uses :: defs :: _) =>
            val nestTree = quiver.empty[u.TermSymbol, Unit, Unit]
              .addNodes(vals.keys.map(x => LNode(x, ()))(breakOut))
              .addNodes(defs.keys.map(m => LNode(m.asTerm, ()))(breakOut))
              .safeAddEdges(nest)

            val ctrlFlow = quiver.empty[u.TermSymbol, u.DefDef, Unit]
              .addNodes(defs.map { case (k, v) => LNode(k.asTerm, v) } (breakOut))
              .safeAddEdges(ctrl)

            val phiNodes = for {
              api.ParSym(lhs) <- vals.keys.toStream if choices.contains(lhs)
              rhs = core.DefCall(module, phi, Seq(lhs.info), Seq(choices(lhs)))
            } yield LNode(lhs, core.ParDef(lhs, rhs))

            val phiEdges = for {
              (lhs, args) <- choices.toStream
              core.Ref(target) <- args if !target.isStatic
            } yield LEdge(target, lhs, ())

            val dataFlow = quiver.empty[u.TermSymbol, u.ValDef, Unit]
              .addNodes(vals.map { case (k, v) => LNode(k, v) } (breakOut))
              .addNodes(phiNodes)
              .safeAddEdges(data)
              .safeAddEdges(phiEdges)

            FlowGraph(uses, nestTree, ctrlFlow, dataFlow)
        }
    }
  }

}
