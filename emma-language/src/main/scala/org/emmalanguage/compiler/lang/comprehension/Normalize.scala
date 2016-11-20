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
      }.andThen(_.tree).andThen(Core.dce).andThen {
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
     * (1) A `flatten` expression occurring in the `expr` position in the enclosing `let` block.
     *
     * {{{
     * {
     *   $vals
     *   flatten {
     *     $outerVals
     *     comprehension {
     *       $outerQs
     *       head {
     *         $innerVals
     *         comprehension {
     *           $innerQs
     *           head $innerHd
     *         } // child comprehension
     *       }
     *     } // outer comprehension
     *   } // parent expression
     * } // enclosing block
     * }}}
     *
     * (2) A `flatten` expression occurring in the `rhs` position of some binding in the enclosing
     * `let` block.
     *
     * {{{
     * {
     *   $enclPre
     *   val x = flatten {
     *     $outerVals
     *     comprehension {
     *       $outerQs
     *       head {
     *         $innerVals
     *         comprehension {
     *           $innerQs
     *           head $innerHd
     *         } // child comprehension
     *       }
     *     } // outer comprehension
     *   } // parent valdef
     *   $enclSuf
     *   $expr // parent expression
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * Let $innerVals decompose into the following two subsets:
     * - $dep (transitively) depends on symbols defined in $outerQs, and
     * - $indep is the independent complement $innerVals \ $dep.
     *
     * For a match of type (1):
     *
     * {{{
     * {
     *   $vals
     *   $outerVals
     *   $indep
     *   comprehension {
     *     $outerQs
     *     $innerQs' // where let blocks are prefixed with $dep
     *     head $innerHd' // where let blocks are prefixed with $dep
     *   } // flattened result comprehension
     * } // enclosing block
     * }}}
     *
     * For a match of type (2):
     *
     * {{{
     * {
     *   $enclPre
     *   $outerVals
     *   $indep
     *   val x = comprehension {
     *     $outerQs
     *     $innerQs' // where let blocks are prefixed with $dep
     *     head $innerHd' // where let blocks are prefixed with $dep
     *   } // flattened result comprehension
     *   $enclSuf
     *   $expr
     * } // enclosing block
     * }}}
     */
    private[Normalize] val UnnestHead: Rule = {
      case Attr.none(core.Let(vals, defs, expr)) =>
        vals.map(v => v -> v.rhs) :+ (expr -> expr) collectFirst {
          case (encl, Flatten(core.Let(outerVals, Seq(),
            Comprehension(outerQs,
              Head(core.Let(innerVals, Seq(),
                DataBagExprToCompr(innerQs, Head(innerHd)))))))) =>

            val (dep, indep) = split(innerVals, outerQs)
            val qs = outerQs ++ innerQs.map(capture(self, dep, prune = false))
            val hd = capture(self, dep, prune = false)(Head(innerHd))
            val flat = Core.inlineLetExprs(Comprehension(qs, hd))
            val (flatVals, flatExpr) = encl match {
              case encl @ core.ValDef(lhs, _) =>
                val (enclPre, enclSuf) = splitAt(encl)(vals)
                val flatVal = core.ValDef(lhs, flat)
                (Seq.concat(enclPre, outerVals, indep, Seq(flatVal), enclSuf), expr)
              case _ =>
                (Seq.concat(vals, outerVals, indep), flat)
            }

            core.Let(flatVals, defs, flatExpr)
        }

      case _ => None
    }

    /**
     * This matches any DataBag expression.
     * If it is not a comprehension, then it wraps it into one.
     */
    object DataBagExprToCompr {
      def unapply(tree: u.Tree): Option[(Seq[u.Tree], u.Tree)] = tree match {
        case Comprehension(qs, hd) => Some(qs, hd)
        case _ if tree.tpe.dealias.widen.typeConstructor =:= Monad =>
          val tpe = api.Type.arg(1, tree.tpe.dealias.widen)
          val lhs = api.TermSym.free(api.TermName.fresh("x"), tpe)
          val rhs = core.Let(expr = tree)
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
     * (1) The `parent` comprehension is an `expr` position in the enclosing `let` block.
     *
     * {{{
     * {
     *   $yPre
     *   val y = comprehension {
     *     $yQs
     *     head $yHd
     *   } // child comprehension
     *   $ySuf
     *   comprehension {
     *     $xPre
     *     val x = generator(y) // gen
     *     $xSuf
     *     head $enclHd
     *   } // parent comprehension
     * } // enclosing block
     * }}}
     *
     * (2) The `parent` comprehension is an `rhs` position for some value definitions in the
     * enclosing `let` block.
     *
     * {{{
     * {
     *   $yPre
     *   val y = comprehension {
     *     $yQs
     *     head $yHd
     *   } // child comprehension
     *   $ySuf
     *   val z = comprehension {
     *     $xPre
     *     val x = generator(y) // gen
     *     $xSuf
     *     head $enclHd
     *   } // parent comprehension
     *   $enclSuf
     *   $expr
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * For a match of type (1):
     *
     * {{{
     * {
     *   $yPre
     *   $ySuf
     *   comprehension {
     *     $xPre
     *     $yQs
     *     $xSuf [ $enclHd \ x ]
     *     head $yHd [ $enclHd \ x ]
     *   } // unnested result comprehension
     * } // enclosing block
     * }}}
     *
     * For a match of type (2):
     *
     * {{{
     * {
     *   $yPre
     *   $ySuf
     *   val z = comprehension {
     *     $xPre
     *     $yQs
     *     $xSuf [ $enclHd \ x ]
     *     head $yHd [ $enclHd \ x ]
     *   } // unnested result comprehension
     *   $enclSuf
     *   $expr
     * } // enclosing block
     * }}}
     */
    private[Normalize] val UnnestGenerator: Rule = {
      case Attr.syn(core.Let(vals, defs, expr), valUses :: _) => (for {
        yVal @ core.ValDef(y, Comprehension(yQs, Head(yHd))) <- vals.view
        if valUses(y) == 1
        (yPre, ySuf) = splitAt(yVal)(vals)
        enclosing = ySuf.view.map(x => x -> x.rhs) :+ (expr -> expr)
        (encl, Comprehension(enclQs, Head(enclHd))) <- enclosing
        xGen @ Generator(x, core.Let(Seq(), Seq(), core.ValRef(`y`))) <- enclQs.view
      } yield {
        // define a substitution function `· [ $hd2 \ x ]`
        val subst = yHd match {
          case core.Let(Seq(), Seq(), wrapped) => api.Tree.subst(Seq(x -> wrapped))
          case _ => api.Tree.subst(Seq(x -> yHd))
        }

        val (xPre, xSuf) = splitAt[u.Tree](xGen)(enclQs)
        val qs = Seq.concat(xPre, yQs, xSuf map subst)
        val flat = Comprehension(qs, Head(asLet(subst(enclHd))))
        val (flatVals, flatExpr) = encl match {
          case encl @ core.ValDef(lhs, _) =>
            val (enclPre, enclSuf) = splitAt(encl)(ySuf)
            val flatVal = core.ValDef(lhs, flat)
            (Seq.concat(yPre, enclPre, Seq(flatVal), enclSuf), expr)
          case _ =>
            (Seq.concat(yPre, ySuf), flat)
        }

        core.Let(flatVals, defs, flatExpr)
      }).headOption

      case _ => None
    }

    private[Normalize] val rules = Seq(
      UnnestGenerator, UnnestHead)
  }
}
