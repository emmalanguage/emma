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

  private lazy val strategy = api.BottomUp.exhaust.withValUses
  private type Rule = Attr[strategy.Acc, strategy.Inh, strategy.Syn] => Option[u.Tree]

  private[comprehension] object Normalize {

    /**
     * Normalizes nested mock-comprehension syntax.
     *
     * @param monad The symbol of the monad syntax to be normalized.
     * @return The normalized input tree.
     */
    def normalize(monad: u.Symbol): u.Tree => u.Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = Syntax(monad)
      val nr = NormalizationRules(cs)
      // apply UnnestHead and UnnestGenerator rules exhaustively
      strategy.transformWith { case attr @ Attr.none(core.Let(_, _, _)) =>
        nr.rules.foldLeft(Option.empty[u.Tree]) {
          (done, rule) => done orElse rule(attr)
        }.getOrElse(attr.tree)
      }.andThen(_.tree).andThen {
        // eliminate trivial guards produced by normalization
        api.BottomUp.transform { case cs.Comprehension(qs, hd) =>
          cs.Comprehension(qs filter {
            case cs.Guard(core.Let(_, _, core.Lit(true))) => false
            case _ => true
          }, hd)
        }.andThen(_.tree)
      }
    }
  }

  private case class NormalizationRules(cs: ComprehensionSyntax) {

    // -------------------------------------------------------------------------
    // Comprehension normalization rules
    // -------------------------------------------------------------------------

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

    /**
     * Un-nests a comprehended generator in its parent.
     *
     * ==Matching Pattern==
     *
     * A `parent` comprehension with a generator that binds to `x` and references a `child`
     * comprehension that occurs in one of the previous value bindings within the enclosing `let`
     * block.
     *
     * {{{
     * {
     *   $xpre
     *   val x = comprehension {
     *     $xqs
     *     head {
     *       $xvs
     *       $alias
     *     }
     *   } // child comprehension
     *   $ypre
     *   val y = comprehension {
     *     $zpre
     *     val z = generator(x) // gen
     *     $zsuf
     *     $yhd
     *   } // parent comprehension
     *   $ysuf
     *   $expr
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * {{{
     * {
     *   $xpre
     *   $ypre
     *   val y = comprehension {
     *     $zpre
     *     $xqs
     *     $zsuf [ x := alias ]
     *     $yhd [ x := alias ]
     *   } // unnested result comprehension
     *   $ysuf
     *   $expr
     * } // enclosing block
     * }}}
     */
    private[Normalize] val UnnestGenerator1: Rule = {
      case Attr.syn(core.Let(vals, defs, expr), uses :: _) => {
        for {
          xv @ core.ValDef(x, cs.Comprehension(xqs, cs.Head(
            core.Let(xvs, Seq(), core.Ref(alias))
          ))) <- vals.view
          if uses(x) == 1
          (xpre, xsuf) = splitAt(xv)(vals)
          yv @ core.ValDef(y, cs.Comprehension(yqs, yhd)) <- xsuf
          gen @ cs.Generator(z, core.Let(Seq(), Seq(), core.Ref(`x`))) <- yqs.view
          (ypre, ysuf) = splitAt(yv)(xsuf)
          (zpre, zsuf) = splitAt[u.Tree](gen)(yqs)
          subst = api.Tree.rename(Seq(z -> alias)).andThen(capture(cs, xvs))
          qs = Seq.concat(zpre, xqs, zsuf.map(subst))
          unnested = Seq(core.ValDef(y, cs.Comprehension(qs, subst(yhd))))
          flatVals = Seq.concat(xpre, ypre, unnested, ysuf)
        } yield core.Let(flatVals, defs, expr)
      }.headOption

      case _ => None
    }

    /**
     * Un-nests a comprehended generator in its parent.
     *
     * ==Matching Pattern==
     *
     * A `parent` comprehension with a generator that binds to `x` and references a `child`
     * comprehension that occurs in one of the previous value bindings within the enclosing `let`
     * block.
     *
     * {{{
     * {
     *   $ypre
     *   val y = comprehension {
     *     $zpre
     *     val z = generator {
     *       $xpre
     *       val x = comprehension {
     *         $xqs
     *         head {
     *           $xvs
     *           $alias
     *         }
     *       } // child comprehension
     *       x
     *     } // gen
     *     $zsuf
     *     $yhd
     *   } // parent comprehension
     *   $ysuf
     *   $expr
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * Let $xvs decompose into the following two subsets:
     * - $zdep (transitively) depends on symbols defined in $zpre, and
     * - $zind is the independent complement $xvs \ $zdep1.
     *
     * {{{
     * {
     *   $ypre
     *   $zind
     *   val y = comprehension {
     *     $zpre
     *     $xqs  // where let blocks are prefixed with $dep
     *     $zsuf [ z := alias ] // where let blocks are prefixed with $dep
     *     $yhd [ z := alias ]  // where let blocks are prefixed with $dep
     *   } // unnested result comprehension
     *   $ysuf
     *   $expr
     * } // enclosing block
     * }}}
     */
    private[Normalize] val UnnestGenerator2: Rule = {
      case Attr.syn(core.Let(vals, defs, expr), uses :: _) => {
        for {
          yv@core.ValDef(y, cs.Comprehension(yqs, yhd)) <- vals.view
          gen@cs.Generator(z, core.Let(xpre :+ xv, Seq(), core.Ref(x))) <- yqs.view
          if uses(x) == 1
          core.ValDef(`x`, cs.Comprehension(xqs, cs.Head(
            core.Let(xvs, Seq(), core.Ref(alias))
          ))) <- Some(xv)
          (ypre, ysuf) = splitAt(yv)(vals)
          (zpre, zsuf) = splitAt[u.Tree](gen)(yqs)
          (xdep, xind) = split(xpre, zpre)
          subst1 = api.Tree.rename(Seq(z -> alias)).andThen(capture(cs, xdep))
          subst2 = api.Tree.rename(Seq(z -> alias)).andThen(capture(cs, xdep ++ xvs))
          qs = Seq.concat(zpre, xqs.map(subst1), zsuf.map(subst2))
          unnested = Seq(core.ValDef(y, cs.Comprehension(qs, subst2(yhd))))
          flatVals = Seq.concat(ypre, xind, unnested, ysuf)
        } yield core.Let(flatVals, defs, expr)
      }.headOption

      case _ => None
    }

    private[Normalize] val rules =
      Seq(UnnestGenerator1, UnnestGenerator2)
  }
}
