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

import compiler.lang.core.Core
import compiler.Common

import shapeless._

import scala.collection.breakOut

private[comprehension] trait Normalize extends Common {
  self: Core with Comprehension =>

  import Comprehension._
  import UniverseImplicits._
  import Core.{Lang => core}

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
      val cs = new Syntax(monad) with NormalizationRules
      // apply UnnestHead and UnnestGenerator rules exhaustively
      strategy.transformWith { case attr @ Attr.none(core.Let(_, _, _)) =>
        cs.rules.foldLeft(Option.empty[u.Tree]) {
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

  protected trait NormalizationRules {
    self: Syntax =>

    // -------------------------------------------------------------------------
    // Comprehension normalization rules
    // -------------------------------------------------------------------------

    /**
     * Unnests a comprehended head in its parent.
     *
     * ==Matching Pattern==
     *
     * {{{
     * {
     *   $pre
     *   val x = flatten {
     *     $xvs
     *     comprehension {
     *       $xqs
     *       head {
     *         $yvs
     *         val y = comprehension {
     *           $yqs
     *           $yhd
     *         } // inner comprehension
     *         y
     *       }
     *     } // outer comprehension
     *   } // parent val
     *   $suf
     *   $defs // parent defs
     *   $expr // parent expression
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * Let $yvs decompose into the following two subsets:
     * - $dep (transitively) depends on symbols defined in $xqs, and
     * - $indep is the independent complement $yvs \ $dep.
     *
     * {{{
     * {
     *   $pre
     *   $xvs
     *   $indep
     *   val x = comprehension {
     *     $xqs
     *     $yqs' // where let blocks are prefixed with $dep
     *     $yhd' // where let blocks are prefixed with $dep
     *   } // flattened result comprehension
     *   $suf
     *   $defs
     *   $expr
     * } // enclosing block
     * }}}
     */
    private[Normalize] val UnnestHead: Rule = {
      case Attr.none(core.Let(vals, defs, expr)) => {
        for {
          xv @ core.ValDef(x, Flatten(
            core.Let(xvs, Seq(), Comprehension(xqs, Head(
              core.Let(yvs :+ core.ValDef(y, AsComprehension(yqs, yhd)), Seq(), core.Ref(z))
          ))))) <- vals.view
          if y == z
          (dep, indep) = split(yvs, xqs)
          qs = xqs ++ yqs.map(capture(self, dep))
          hd = capture(self, dep)(yhd)
          (pre, suf) = splitAt(xv)(vals)
          unnested = Seq(core.ValDef(x, Comprehension(qs, hd)))
          flatVals = Seq.concat(pre, xvs, indep, unnested, suf)
        } yield core.Let(flatVals, defs, expr)
      }.headOption

      case _ => None
    }

    /**
     * This matches any DataBag expression.
     * If it is not a comprehension, then it wraps it into one.
     */
    object AsComprehension {
      def unapply(tree: u.Tree): Option[(Seq[u.Tree], u.Tree)] = tree match {
        case Comprehension(qs, hd) => Some(qs, hd)
        case _ if tree.tpe.dealias.widen.typeConstructor =:= Monad =>
          val tpe = api.Type.arg(1, tree.tpe.dealias.widen)
          val lhs = api.TermSym.free(api.TermName.fresh(), tpe)
          val rhs = asLet(tree)
          val ref = core.Let(expr = api.TermRef(lhs))
          Some(Seq(Generator(lhs, rhs)), Head(ref))
        case _ => None
      }
    }

    /** Splits `vals` in two: vals dependent on generators bound in `qs`, and complement. */
    private def split(vals: Seq[u.ValDef], qs: Seq[u.Tree]): (Seq[u.ValDef], Seq[u.ValDef]) = {
      // symbols referenced in vals
      val valRefs = for (core.ValDef(lhs, rhs) <- vals)
        yield lhs -> api.Tree.refs(rhs)

      var dependent: Set[u.TermSymbol] = qs.collect {
        case Generator(lhs, _) => lhs
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
    private[Normalize] val UnnestGenerator: Rule = {
      case Attr.syn(core.Let(vals, defs, expr), uses :: _) => {
        for {
          xv @ core.ValDef(x, Comprehension(xqs, Head(
            core.Let(xvs, Seq(), core.Ref(alias))
          ))) <- vals.view
          if uses(x) == 1
          (xpre, xsuf) = splitAt(xv)(vals)
          yv @ core.ValDef(y, Comprehension(yqs, yhd)) <- xsuf
          gen @ Generator(z, core.Let(Seq(), Seq(), core.Ref(`x`))) <- yqs.view
          (ypre, ysuf) = splitAt(yv)(xsuf)
          (zpre, zsuf) = splitAt[u.Tree](gen)(yqs)
          subst = api.Tree.rename(Seq(z -> alias)).andThen(capture(self, xvs))
          qs = Seq.concat(zpre, xqs, zsuf.map(subst))
          unnested = Seq(core.ValDef(y, Comprehension(qs, subst(yhd))))
          flatVals = Seq.concat(xpre, ypre, unnested, ysuf)
        } yield core.Let(flatVals, defs, expr)
      }.headOption

      case _ => None
    }

    private[Normalize] val rules =
      Seq(UnnestGenerator, UnnestHead)
  }
}
