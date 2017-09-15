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
import compiler.lang.cf.ControlFlow
import compiler.lang.comprehension.Comprehension
import compiler.lang.core.Core

import collection.breakOut
import scala.annotation.tailrec

private[backend] trait Context extends Common {
  self: Core with ControlFlow with Comprehension =>

  import API._
  import Core.{Lang => core}
  import UniverseImplicits._

  object Context {

    type Graph = quiver.Graph[u.TermSymbol, BCtx, Boolean]

    /**
     * Annotates the dataflow graph derived from the tree.
     * - nodes are labeled with their binding context value.
     * - edges are labeled with a boolean flag inidcating use in a data-parallel operator.
     */
    def bCtxGraph(tree: u.Tree): Graph = {
      bCtxGraph(ControlFlow.cfg(tree))
    }

    /**
     * Annotates the dataflow graph.
     * - nodes are labeled with their binding context value.
     * - edges are labeled with a boolean flag inidcating use in a data-parallel operator.
     */
    def bCtxGraph(G: FlowGraph[u.TermSymbol]): Graph = {
      val bCtx = bCtxMap(G)
      G.data.gmap(g => g.copy(
        inAdj = g.inAdj.map { case (_, from) =>
          (isDataParallelCall(from, g.label), from)
        },
        outAdj = g.outAdj.map { case (_, to) =>
          (isDataParallelCall(g.vertex, G.data.label(to).get), to)
        }
      )).nmap({
        case core.BindingDef(lhs, _) => bCtx(lhs)
      })
    }

    /** Computes a binding context of all [[core.BindingDef]] nodes defined in an input tree. */
    def bCtxMap(tree: u.Tree): Map[u.TermSymbol, BCtx] = {
      bCtxMap(ControlFlow.cfg(tree))
    }

    /** Computes the binding context of all [[core.BindingDef]] nodes defined in an input graph. */
    def bCtxMap(G: FlowGraph[u.TermSymbol], strict: Boolean = true): Map[u.TermSymbol, BCtx] = {
      // 'enclosing symbol' map
      val E = {
        val parSymb = G.nest.edges.map(
          e => e.to -> e.from
        )(breakOut): Map[u.TermSymbol, u.TermSymbol]
        @tailrec
        def encSymb(sym: u.TermSymbol): Option[u.TermSymbol] =
          parSymb.get(sym) match {
            case Some(par) => G.data.label(par) match {
              case Some(core.ValDef(`par`, core.Lambda(_, _, _))) => Some(par)
              case Some(core.ValDef(`par`, cs.Comprehension(_, _))) => Some(par)
              case _ => encSymb(par) // bypass parent
            }
            case _ => None // no parent
          }

        (for {
          sym <- G.data.nodes
          enc <- encSymb(sym)
        } yield {
          sym -> enc
        })(breakOut): Map[u.TermSymbol, u.TermSymbol]
      }
      // dataflow graph where edges `x -> y` are annotated with a boolean label indicating
      // whether `x` denotes a lambda used as a parameter of a data-parallel operator
      // call denoted by `y` (e.g., `map`, `flatMap`, `withFilter`, `groupBy`)
      val P = G.data.gmap(g => g.copy(
        inAdj = g.inAdj.map { case (_, from) =>
          (isDataParallelCall(from, g.label), from)
        },
        outAdj = g.outAdj.map { case (_, to) =>
          (isDataParallelCall(g.vertex, G.data.label(to).get), to)
        }
      ))

      val bCtxt = collection.mutable.Map[u.TermSymbol, BCtx]()
      var delta = Seq.empty[(u.TermSymbol, BCtx)]

      delta = for {
        x <- P.nodes if !(E contains x)
      } yield {
        x -> BCtx.Driver // `x` without enclosing `y`
      }

      while (delta.nonEmpty) {
        bCtxt ++= delta
        delta = for {
          x <- P.nodes if !(bCtxt contains x) // def. with undecided binding context
          y <- E.get(x) if bCtxt contains y // enclosing def. with decided binding context
          l <- P.label(y) // value definition of `y`
        } yield {
          // compute the binding context value for `x`
          val v =
            if (bCtxt(y) != BCtx.Driver) bCtxt(y) // inerit from parent for all but `Driver`
            else l match {
              case core.ValDef(`y`, core.Lambda(_, _, _)) =>
                // `y` denotes a lambda => binding context of values depends on call sites of `y`
                val edges = P.outEdges(y)
                if (edges.isEmpty) BCtx.Driver
                else edges.map(edge =>
                  if (edge.label) BCtx.Engine
                  else BCtx.Driver
                ).reduce(_ | _)
              case core.ValDef(`y`, cs.Comprehension(_, _)) =>
                // `y` denotes a comprehension => binding context is always `Engine`
                BCtx.Engine
            }
          x -> v
        }
      }

      if (strict) for (s <- G.data.nodes) bCtxt.get(s) match {
        case None =>
          abort(s"Cannot determine binding context of $s", s.pos)
        case Some(BCtx.Ambiguous) =>
          abort(s"Ambiguous binding context of $s", s.pos)
        case Some(_) => // do nothing
      }

      bCtxt.toMap.withDefault(_ => BCtx.Unknown)
    }

    private lazy val cs = Comprehension.Syntax(DataBag.sym)

    private lazy val Alg = api.Sym[org.emmalanguage.api.alg.Alg[Any, Any]]

    private lazy val dataParallelOps = {
      import API.DataBag._
      val bagOps = monadOps union nestOps union foldOps
      val mutableBagOps = API.MutableBag.ops
      val combinatorOps = API.ComprehensionCombinators.ops
      bagOps union mutableBagOps union combinatorOps
    }

    private def isDataParallelCall(x: u.TermSymbol, vd: u.ValDef): Boolean =
      api.Sym.funs(x.info.typeSymbol) && (vd match {
        case core.ValDef(_, core.DefCall(_, m, _, argss))
          if isDataParallelOp(m) =>
          inArgs(x, argss)
        case core.ValDef(_, core.DefCall(Some(core.Ref(t)), m, _, argss))
          if m.name == api.TermName.app && isAlgCompanion(t) =>
          inArgs(x, argss)
        case _ => false
      })

    private def isDataParallelOp(m: u.MethodSymbol): Boolean =
      (m :: m.overrides).exists({
        case api.DefSym(s) => dataParallelOps(s)
        case _ => false
      })

    private def isAlgCompanion(t: u.TermSymbol): Boolean = {
      t.companion.isClass
    } && {
      t.companion.asClass.baseClasses contains Alg
    }

    private def inArgs(x: u.TermSymbol, argss: Seq[Seq[u.Tree]]): Boolean =
      argss.flatten exists {
        case core.Ref(`x`) => true
        case _ => false
      }
  }

}
