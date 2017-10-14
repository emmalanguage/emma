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
package compiler.opt

import compiler.Common
import compiler.lang.cf.ControlFlow
import compiler.lang.comprehension.Comprehension
import compiler.lang.core.Core
import util.Graphs.topoSort

import quiver.LNode

import scala.collection.SortedMap
import scala.collection.breakOut

/** The fold-fusion optimization. */
private[compiler] trait FoldGroupFusion extends Common {
  self: Core with Comprehension with ControlFlow =>

  /** The fold-fusion optimization. */
  private[compiler] object FoldGroupFusion {

    private type Node = LNode[u.TermSymbol, u.ValDef]
    private type MMap = Map[u.TermSymbol, Match]

    import API._
    import Core.{Lang => core}
    import UniverseImplicits._

    val cs = Comprehension.Syntax(DataBag.sym)

    /**
     * Performs the fold-group-fusion optimization on `DataBag` expressions.
     *
     * Looks for patterns of the following shape:
     *
     * {{{
     *   //... (vals1)
     *   val x = y.groupBy(k)
     *   //... (vals2)
     *   val c = for (g <- x) yield {
     *     //...
     *     // <dependencies of `a` inside `c`>
     *     val v = g.values
     *     val f = v.fold(a)
     *     //...
     *   }
     * }}}
     *
     * where
     *
     * - the only use of `x` is in `c`;
     * - the only use of `v` is in `f`;
     * - the the algebra `a` does not (transitively) depend on `g`.
     *
     * Matches are rewritten as follows.
     *
     * {{{
     *   //... (vals1)
     *   //... (vals2)
     *   // <dependencies of `a` inside `c`>
     *   val x = LocalOps.foldGroup(y, k, a)
     *   val c = for (g <- x) yield {
     *     // ...
     *     val f = g.values
     *     // ...
     *   }
     * }}}
     */
    lazy val foldGroupFusion = TreeTransform("FoldGroupFusion.foldGroupFusion", tree => {
      val cfg = ControlFlow.cfg(tree)
      val dat = cfg.data
      val nst = cfg.nest.tclose

      val ms = for {
        GroupBy(x, y, k) <- dat.labNodes.map(_.label)
        if cfg.uses(x) == 1
        xUses = dat.successors(x).flatMap(nst.predecessors).flatMap(dat.label)
        GroupComprehension(c, g, `x`) <- xUses
        gUses = dat.successors(g).flatMap(dat.label)
        ProjValues(v, g) <- gUses
        if cfg.uses(v) == 1
        vUses = dat.successors(v).flatMap(dat.label)
        FoldValues(f, v, a) <- vUses
        deps = dat.reverse
        d = deps.reachable(a).toSet
        if !(d contains g)
      } yield {
        val C = cfg.nest.outs(c).map(_._2).toSet
        val dm = SortedMap((for {
          v <- deps.nodes
          if d contains v
        } yield v -> deps.outs(v).map(_._2).toSet): _*)(ordTermSymbol)
        val ds = for {
          v <- topoSort(dm).get
          if C contains v
          w <- dat.label(v)
        } yield w
        Match(x, y, k, c, g, v, f, a, ds)
      }

      val xMap: MMap = ms.map(m => m.x -> m)(breakOut)
      val gMap: MMap = ms.map(m => m.g -> m)(breakOut)
      val vMap: MMap = ms.map(m => m.v -> m)(breakOut)
      val fMap: MMap = ms.map(m => m.f -> m)(breakOut)

      // Unsafe transformation, as the type of fused `g` changes from
      // `Group[K, DataBag[A]]` to `Group[K, B]`.
      api.TopDown.unsafe.transform({
        case core.Let(vals, defs, expr) if containsSymbol(xMap.keySet, vals) =>
          val vSet = vals.map(_.symbol.asTerm)(breakOut): Set[u.TermSymbol]
          // we could have more than one match in the same let block
          val vals3 = ms.foldLeft(vals)((vals, m) => if (!vSet(m.x)) vals else {
            val (vals1, res1) = vals.span(_.symbol != m.x)
            val (vals2, res2) = res1.tail.span(_.symbol != m.c)
            val xVal = res1.head
            vals1 ++ vals2 ++ m.d ++ Seq(xVal) ++ res2
          })
          core.Let(vals3, defs, expr)
        case core.Let(vals, defs, expr) if containsSymbol(fMap.keySet, vals) =>
          val ds = for {
            v <- vals.map(_.symbol.asTerm).toSet[u.TermSymbol]
            m <- fMap.get(v).toSet[Match]
            d <- m.d.map(_.symbol.asTerm).toSet[u.TermSymbol]
          } yield d
          core.Let(vals.filterNot(v => ds(v.symbol.asTerm)), defs, expr)
        case core.ValDef(lhs, _) if xMap contains lhs =>
          val mat = xMap(lhs)
          val tgt = Some(core.Ref(API.Ops.sym))
          val met = API.Ops.foldGroup
          val tgs = Seq(mat.A, mat.B, mat.K)
          val ags = Seq(mat.y, mat.k, mat.a).map(core.Ref(_))
          val rhs = core.DefCall(tgt, met, tgs, Seq(ags))
          core.ValDef(mat.xNew, rhs)
        case core.ValDef(lhs, _) if fMap contains lhs =>
          val mat = fMap(lhs)
          val tgt = Some(core.Ref(mat.gNew))
          val met = API.Group.values
          val rhs = core.DefCall(tgt, met, Seq(), Seq())
          core.ValDef(lhs, rhs)
        case cs.Generator(lhs, _) if gMap contains lhs =>
          val mat = gMap(lhs)
          val rhs = core.Let(Seq(), Seq(), core.Ref(mat.xNew))
          cs.Generator(mat.gNew, rhs)
        case core.ValDef(lhs, _) if vMap contains lhs =>
          api.Empty()
        case core.ValRef(sym) if xMap contains sym =>
          val mat = xMap(sym)
          core.Ref(mat.xNew)
        case core.ValRef(sym) if gMap contains sym =>
          val mat = gMap(sym)
          core.Ref(mat.gNew)
      })._tree(tree)
    })

    private val ordTermSymbol = Ordering.by((s: u.TermSymbol) => s.name.toString)

    private def containsSymbol(xs: Set[u.TermSymbol], vals: Seq[u.ValDef]): Boolean =
      vals.exists(v => xs(v.symbol.asTerm))

    /**
     * Represents a chain of symbols to be fused.
     *
     * @param x A `x` matched from a `val x = y.groupBy(k)` value definition.
     * @param y A `y` matched from a `val x = y.groupBy(k)` value definition.
     * @param k A `k` matched from a `val x = y.groupBy(k)` value definition.
     * @param c A `c` matched from a `val c = for (g <- { x }) yield $expr` value definition.
     * @param g A `g` matched from a `val c = for (g <- { x }) yield $expr` value definition.
     * @param v A `v` matched from a `val v = g.values` value definition.
     * @param f A `f` matched from a `val f = v.fold(a)` value definition.
     * @param a A `a` matched from a `val f = v.fold(a)` value definition.
     * @param d Dependencies of `a` to be pulled outside `c`.
     */
    private case class Match
    (
      x: u.TermSymbol,
      y: u.TermSymbol,
      k: u.TermSymbol,
      c: u.TermSymbol,
      g: u.TermSymbol,
      v: u.TermSymbol,
      f: u.TermSymbol,
      a: u.TermSymbol,
      d: Seq[u.ValDef]
    ) {
      val K = api.Type.arg(1, g.info.widen)
      val A = api.Type.arg(1, y.info.widen)
      val B = f.info.widen
      val G = api.Type(API.Group.tpe, Seq(K, B))
      val xNew = api.TermSym.apply(x.owner, x.name, api.Type(API.DataBag.tpe, Seq(G)))
      val gNew = api.TermSym.apply(g.owner, g.name, G)
    }

    private object GroupBy {
      def unapply(vd: u.ValDef): Option[(u.TermSymbol, u.TermSymbol, u.TermSymbol)] = vd match {
        case core.ValDef(x, core.DefCall(Some(core.Ref(y)), m, _, Seq(Seq(core.Ref(k)))))
          if m == DataBag.groupBy => Some(x, y, k)
        case _ => None
      }
    }

    private object GroupComprehension {
      def unapply(vd: u.ValDef): Option[(u.TermSymbol, u.TermSymbol, u.TermSymbol)] = vd match {
        case core.ValDef(c, cs.Comprehension(Seq(GroupGenerator(g, x)), _)) => Some(c, g, x)
        case _ => None
      }
    }

    private object GroupGenerator {
      def unapply(vd: u.ValDef): Option[(u.TermSymbol, u.TermSymbol)] = vd match {
        case cs.Generator(g, SimpleLet(x)) => Some(g, x)
        case _ => None
      }
    }

    private object ProjValues {
      def unapply(vd: u.ValDef): Option[(u.TermSymbol, u.TermSymbol)] = vd match {
        case core.ValDef(v, core.DefCall(Some(core.Ref(g)), m, Seq(), Seq()))
          if m == Group.values => Some(v, g)
        case _ => None
      }
    }

    private object FoldValues {
      def unapply(vd: u.ValDef): Option[(u.TermSymbol, u.TermSymbol, u.TermSymbol)] = vd match {
        case core.ValDef(f, core.DefCall(Some(core.Ref(v)), m, _, Seq(Seq(core.Ref(a)))))
          if m == DataBag.fold1 => Some(f, v, a)
        case _ => None
      }
    }

    private object SimpleLet {
      def unapply(let: u.Block): Option[u.TermSymbol] = let match {
        case core.Let(Seq(), Seq(), core.Ref(x)) =>
          Some(x)
        case _ => None
      }
    }

  }

}
