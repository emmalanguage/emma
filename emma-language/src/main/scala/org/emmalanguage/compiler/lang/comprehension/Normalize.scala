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
     *   $vals1
     *   flatten {
     *     $vals2
     *     comprehension {
     *       $qs1
     *       head {
     *         $vals3
     *         comprehension {
     *           $qs2
     *           head $hd2
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
     *   $vals1a
     *   val x = flatten {
     *     $vals2
     *     comprehension {
     *       $qs1
     *       head {
     *         $vals3
     *         comprehension {
     *           $qs2
     *           head $hd2
     *         } // child comprehension
     *       }
     *     } // outer comprehension
     *   } // parent valdef
     *   $vals1b
     *   $expr1 // parent expression
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * Let $vals3 decompose into the following two subsets:
     * - $vals3i (transitively) depends on symbols defined in $qs1, and
     * - $vals3o is the independent complement $vals3 \ $vals3i.
     *
     * For a match of type (1):
     *
     * {{{
     * {
     *   $vals1
     *   $vals2
     *   $vals3o
     *   comprehension {
     *     $qs1
     *     $qs2' // where let blocks are prefixed with $vals3i
     *     head $hd' // where let blocks are prefixed with $vals3i
     *   } // flattened result comprehension
     * } // enclosing block
     * }}}
     *
     * For a match of type (2):
     *
     * {{{
     * {
     *   $vals1a
     *   $vals2
     *   $vals3o
     *   val x = comprehension {
     *     $qs1
     *     $qs2' // where let blocks are prefixed with $vals3i
     *     head $hd' // where let blocks are prefixed with $vals3i
     *   } // flattened result comprehension
     *   $vals1b
     *   $expr1
     * } // enclosing block
     * }}}
     */
    private[Normalize] val UnnestHead: Rule = {
      case Attr.none(core.Let(vals, defs, expr)) =>
        vals.map(v => v -> v.rhs) :+ (expr -> expr) collectFirst {
          case (encl, Flatten(core.Let(enclVals, Seq(),
            outer @ Comprehension(outerQs,
              Head(core.Let(outerVals, Seq(),
                DataBagExprToCompr(innerQs, Head(innerHd)))))))) =>

            val (dep, indep) = split(outerVals, outerQs)
            val qs = outerQs ++ innerQs.map(capture(self, dep, prune = false))
            val hd = capture(self, dep, prune = false)(Head(innerHd))
            val flat = Core.inlineLetExprs(Comprehension(qs, hd))
            val (flatVals, flatExpr) = encl match {
              case encl @ core.ValDef(lhs, _) =>
                val (enclPre, enclSuf) = splitAt(encl)(vals)
                val flatVal = core.ValDef(lhs, flat)
                (Seq.concat(enclPre, enclVals, indep, Seq(flatVal), enclSuf), expr)
              case _ =>
                (Seq.concat(vals, enclVals, indep), flat)
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
      val refMap = (for {
        core.ValDef(lhs, rhs) <- vals
      } yield lhs -> api.Tree.refs(rhs)).toMap

      // symbols defined in vals which directly depend on symbols from qs
      var dependent = (for {
        Generator(lhs, _) <- qs
      } yield lhs).toSet

      // compute the transitive closure of dependent, i.e. extend with indirect dependencies
      var delta = 0
      do {
        val size = dependent.size
        dependent ++= (for {
          (lhs, refs) <- refMap
          if refs.exists(dependent)
        } yield lhs)
        delta = dependent.size - size
      } while (delta > 0)
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
     *   $vals1a
     *   val y = comprehension {
     *     $qs2
     *     head $hd2
     *   } // child comprehension
     *   $vals1b
     *   comprehension {
     *     $qs1a
     *     val x = generator(y) // gen
     *     $qs1b
     *     head $hd1
     *   } // parent comprehension
     * } // enclosing block
     * }}}
     *
     * (2) The `parent` comprehension is an `rhs` position for some value definitions in the
     * enclosing `let` block.
     *
     * {{{
     * {
     *   $vals1a
     *   val y = comprehension {
     *     $qs2
     *     head $hd2
     *   } // child comprehension
     *   $vals1b
     *   val z = comprehension {
     *     $qs1a
     *     val x = generator(y) // gen
     *     $qs1b
     *     head $hd1
     *   } // parent comprehension
     *   $vals1c
     *   $expr1
     * } // enclosing block
     * }}}
     *
     * ==Rewrite==
     *
     * For a match of type (1):
     *
     * {{{
     * {
     *   $vals1a
     *   $vals1b
     *   comprehension {
     *     $qs1a
     *     $qs2
     *     $qs1b [ $hd2 \ x ]
     *     head $hd1 [ $hd2 \ x ]
     *   } // unnested result comprehension
     * } // enclosing block
     * }}}
     *
     * For a match of type (2):
     *
     * {{{
     * {
     *   $vals1a
     *   $vals1b
     *   val z = comprehension {
     *     $qs1a
     *     $qs2
     *     $qs1b [ $hd2 \ x ]
     *     head $hd1 [ $hd2 \ x ]
     *   } // unnested result comprehension
     *   $vals1c
     *   $expr1
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
        gen @ Generator(x, core.Let(Seq(), Seq(), core.ValRef(`y`))) <- enclQs.view
      } yield {
        // define a substitution function `· [ $hd2 \ x ]`
        val subst = yHd match {
          case core.Let(Seq(), Seq(), wrapped) => api.Tree.subst(Seq(x -> wrapped))
          case _ => api.Tree.subst(Seq(x -> yHd))
        }

        val (genPre, genSuf) = splitAt[u.Tree](gen)(enclQs)
        val qs = Seq.concat(genPre, yQs, genSuf map subst)
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
