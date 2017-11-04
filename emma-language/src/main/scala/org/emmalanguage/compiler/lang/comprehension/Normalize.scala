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
package compiler.lang.comprehension

import compiler.Common
import compiler.lang.core.Core

import shapeless._

import scala.collection.breakOut

private[comprehension] trait Normalize extends Common {
  self: Core =>

  import Comprehension._
  import Core.{Lang => core}
  import UniverseImplicits._

  private lazy val strategy = api.BottomUp.exhaust.withParent.withValUses
  private type Rule = Attr[strategy.Acc, strategy.Inh, strategy.Syn] => Option[u.Tree]

  private[comprehension] object Normalize {

    /**
     * Normalizes nested mock-comprehension syntax.
     *
     * @param monad The symbol of the monad syntax to be normalized.
     * @return The normalized input tree.
     */
    def normalize(monad: u.Symbol) = TreeTransform("Normalize.normalize", {
      // construct comprehension syntax helper for the given monad
      val cs = Syntax(monad)
      val nr = NormalizationRules(cs)
      // apply the UnnestGenerator rule exhaustively
      strategy.transformWith({ case attr@Attr.inh(_: u.Block, parent :: _)
        if nr.isValid(parent) => nr.UnnestGenerator(attr).getOrElse(attr.tree)
      })._tree
    })
  }

  private case class NormalizationRules(cs: ComprehensionSyntax) {

    // -------------------------------------------------------------------------
    // Comprehension normalization rules
    // -------------------------------------------------------------------------

    /**
     * Un-nests a comprehended generator in its parent.
     *
     * ==Matching Pattern==
     *
     * Match a root let block
     *
     * {{{
     * {
     *   $vdefs1
     *   val $x1 = for {
     *     $qs1
     *     $x3 <- { $vdefs2; $x2 }
     *     $qs3
     *   } yield $let1
     *   $vdefs3
     *   $ddefs
     *   $c
     * }
     * }}}
     *
     * where `$x3` is defined either in `$vdefs1` or in `$vdefs2` as follows.
     *
     * {{{
     * val $x2 = for { $qs2 } yield $let2
     * }}}
     *
     * ==Conditions==
     *
     * `$x2` is referenced only once (in the matched let expression).
     *
     * ==Rewrite==
     *
     * Split `$vdefs2` (excluding the `$x2` definition) in two subsequences
     *
     * - `$vdefs2D` - entries that (transitively) depend on `$qs1`.
     * - `$vdefs2I` - entries that do not (transitively) depend on `$qs1`;
     *
     * Let
     *
     * {{{
     * val $qs2'  = $qs2 map fix
     * val $qs3'  = $qs3 map fix
     * val $let1' = fix($let1)
     * }}}
     *
     * where `fix($x)`
     * - eliminates `$x3` by inlining `$let2` if necessary (maintaining Emma Core), and
     * - prepends `$vdefs2D` definitions referended in `$x` to the above result.
     *
     * The result of unnesting `$x2` in `$x1` is constructed as follows
     *
     * {{{
     * {
     *   $vdefs1'
     *   val $x1 = for {
     *     $qs1
     *     $qs2'
     *     $qs3'
     *   } yield $let1'
     *   $vdefs3
     *   $ddefs
     *   $c
     * }
     * }}}
     *
     * where `$vdefs1'` is obtained from `$vdefs1` by removing the `$x2` definition.
     */
    private[Normalize] val UnnestGenerator: Rule = {
      case Attr.syn(core.Let(vdefs, ddefs, c), uses :: _) => {
        for {
          v@core.ValDef(x1, cs.Comprehension(qs, cs.Head(let1))) <- vdefs.view
          q@cs.Generator(x3, core.Let(vdefs2, Seq(), core.Ref(x2))) <- qs.view
          if uses(x2) == 1
          (vdefs1, vdefs3) = splitAt(v)(vdefs)
          core.ValDef(`x2`, cs.Comprehension(qs2, cs.Head(let2))) <- (vdefs1 ++ vdefs2).view
        } yield {
          val (qs1, qs3) = splitAt[u.Tree](q)(qs)
          val (vdefs2D, vdefs2I) = split(vdefs2.filterNot(_.symbol == x2), qs)

          val fix = (tree: u.Tree) => capture(cs, vdefs2D)(tree match {
            case cs.Generator(lhs, rhs) => cs.Generator(lhs, inline(x3, let2)(rhs))
            case cs.Guard(pred) => cs.Guard(inline(x3, let2)(pred))
            case cs.Head(let) => cs.Head(inline(x3, let2)(let))
          })

          core.Let(Seq.concat(
            vdefs1.filterNot(_.symbol == x2),
            vdefs2I,
            Seq(core.ValDef(x1, removeTrivialGuards(cs.Comprehension(Seq.concat(
              qs1,
              qs2.map(fix),
              qs3.map(fix)),
              fix(cs.Head(let1)))))),
            vdefs3),
            ddefs,
            c)
        }
      }.headOption

      case _ => None
    }

    /* Eliminates trivial guards produced by pattern matching generator lhs */
    private[Normalize] val removeTrivialGuards: u.Tree =?> u.Tree = {
      case cs.Comprehension(qs, hd) =>
        cs.Comprehension(qs filter {
          case cs.Guard(core.Let(_, _, core.Lit(true))) => false
          case _ => true
        }, hd)
    }

    /** Checks whether a let block is a valid rewrite root (this depents on its parent).  */
    private[Normalize] def isValid(parent: Option[u.Tree]): Boolean = parent match {
      case Some(core.DefDef(_, _, _, _)) => true
      case None => true
      case _ => false
    }

    /**
     * Extends let block `dfn` which defines `sym` with a let block `use` which uses `sym`,
     * maintaining Emma Core. If `use` does not refer to `sym`, returns `use` unmodified.
     */
    private def inline(sym: u.TermSymbol, dfn: u.Block)(use: u.Block): u.Block =
      if (!(api.Tree.refs(use) contains sym)) use
      else {
        val core.Let(vals2, defs2, expr2) = use
        Core.mapSuffix(dfn, Some(use.tpe)) { case (vals1, expr1) =>
          val fix = expr1 match {
            case core.Ref(tgt) => api.Tree.rename(Seq(sym -> tgt))
            case tgt => api.Tree.subst(Seq(sym -> tgt))
          }
          fix(core.Let(vals1 ++ vals2, defs2, expr2)).asInstanceOf[u.Block]
        }
      }

    /** Splits `vals` in two: vals dependent on generators bound in `qs`, and complement. */
    private def split(vals: Seq[u.ValDef], qs: Seq[u.Tree]): (Seq[u.ValDef], Seq[u.ValDef]) = {
      // symbols referenced in vals
      val valRefs = for (core.ValDef(lhs, rhs) <- vals)
        yield lhs -> api.Tree.refs(rhs)

      var dependent: Set[u.TermSymbol] = qs.collect {
        case cs.Generator(lhs, _) => lhs
      } (breakOut)
      var size = 0
      while (size != dependent.size) {
        size = dependent.size
        dependent ++= (for {
          (lhs, refs) <- valRefs
          if refs exists dependent
        } yield lhs)
      }

      vals.partition(dependent.compose(_.symbol.asTerm))
    }
  }
}
