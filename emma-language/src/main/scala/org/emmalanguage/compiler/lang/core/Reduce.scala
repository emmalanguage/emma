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
import util.Monoids.overwrite

import cats._
import shapeless._

import scala.annotation.tailrec

private[core] trait Reduce extends Common {
  self: Core =>

  import Core.{Lang => core}
  import UniverseImplicits._

  private case class Lam(params: Seq[u.ValDef], body: u.Block)

  private[core] object Reduce {

    type ValDefs = Map[u.Symbol, u.Tree]

    /**
     * Reduces an Emma Core term as follows.
     *
     * - Inlines local lambda definitions (without local control-flow) which are
     * used only once in an application.
     * - Propagates trivial assignments.
     *
     * == Preconditions ==
     * - The input tree is in LNF (see [[Core.lnf]]).
     *
     * == Postconditions ==
     * - All unused value definitions are pruned.
     */
    lazy val transform: TreeTransform = TreeTransform("Reduce.transform", Seq(
      TreeTransform("Reduce.fixInlineLambdas", fixInlineLambdas _),
      inlineTrivialValDefs
    ))

    private lazy val inlineTrivialValDefs = TreeTransform("Reduce.inlineTrivialValDefs",
      api.BottomUp.inherit({ // accumulate trivial ValDef bindings in scope
        case core.Let(vals, _, _) =>
          vals.foldLeft(Map.empty[u.Symbol, u.Tree])((valDefs, valDef) => valDef match {
            case core.ValDef(x, a@core.Ref(y)) if !x.isImplicit => valDefs + (x -> valDefs.getOrElse(y, a))
            case core.ValDef(x, a@core.Atomic(_)) if !x.isImplicit => valDefs + (x -> a)
            case _ => valDefs
          })
      })(mclose).transformWith({
        case Attr.inh(core.Ref(x), valDefs :: _)
          if valDefs contains x => valDefs(x)
        case Attr.none(core.Let(vals, defs, expr)) =>
          core.Let(vals.filter((vd: u.ValDef) => vd.symbol.isImplicit || (vd.rhs match {
            case core.Atomic(_) => false
            case _ => true
          })), defs, expr)
      })._tree)

    /** Merges two closed trivial ValDef maps preserving the closure. */
    private def mclose: Monoid[ValDefs] = new Monoid[ValDefs] {
      def empty = Map.empty
      def combine(x: ValDefs, y: ValDefs) = x ++ y.mapValues({
        case a@core.Ref(v) => x.getOrElse(v, a)
        case a => a
      })
    }

    @tailrec
    private def fixInlineLambdas(x: u.Tree): u.Tree = {
      val y = inlineLambdas(x)
      if (x == y) y
      else fixInlineLambdas(y)
    }

    private lazy val inlineLambdas: u.Tree => u.Tree = {
      val valReplBuilder = Map.newBuilder[u.TermSymbol, Seq[u.ValDef]]
      val inlinedBuilder = Set.newBuilder[u.TermSymbol]

      api.BottomUp.withValUses.synthesize(Attr.group {
        case core.ValDef(lhs, core.Lambda(_, params, body@core.Let(_, Seq(), _))) =>
          lhs -> Lam(params, body)
      })(overwrite).withRoot.transformSyn {
        case Attr(let@core.Let(vals, defs, expr), _, root :: _, syn) =>
          val lamDefs :: valUses :: _ = syn(root getOrElse let)

          valReplBuilder.clear()
          inlinedBuilder.clear()

          for {
            core.ValDef(x, core.DefCall(Some(core.Ref(f)), m, Seq(), Seq(args))) <- vals
            if m.name == api.TermName.app
            l <- lamDefs.get(f)
            if valUses(f) == 1
          } {
            // create substitution function
            val subst = {
              api.Tree.subst(l.params.map(_.symbol) zip args)
            } andThen (l.body.expr match {
              case core.Ref(y) => api.Sym.subst(x.owner, Seq(y -> x))
              case _ => api.Sym.subst(x.owner, Seq())
            })
            // mark `f` as inlined
            inlinedBuilder += f
            // and resulting vals for `x`
            valReplBuilder += x -> (subst(l.body) match {
              case core.Let(rvals, _, rexpr) => rexpr match {
                case core.Ref(`x`) => rvals
                case _ => rvals :+ core.ValDef(x, rexpr)
              }
            })
          }

          val valRepl = valReplBuilder.result()
          val inlined = inlinedBuilder.result()

          val vals1 = for {
            v@core.ValDef(x, _) <- vals
            x <- {
              if (valRepl contains x) {
                valRepl(x) // beta-reduced function application
              } else if ((lamDefs contains x) && (valUses(x) < 1 || (inlined contains x))) {
                Seq() // unused function after inlining
              } else {
                Seq(v) // default case - do nothing
              }
            }
          } yield x

          if (vals == vals1) let
          else core.Let(vals1, defs, expr)
      }._tree
    }
  }

}
