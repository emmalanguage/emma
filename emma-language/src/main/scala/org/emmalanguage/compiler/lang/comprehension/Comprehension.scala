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

trait Comprehension extends Common
  with ReDeSugar
  with Normalize
  with Combination {
  self: Core =>

  import UniverseImplicits._
  import Core.{Lang => core}

  private[compiler] object Comprehension {

    // -------------------------------------------------------------------------
    // Mock comprehension syntax language
    // -------------------------------------------------------------------------

    trait MonadOp {
      val symbol: u.TermSymbol

      def apply(xs: u.Tree)(fn: u.Tree): u.Tree

      def unapply(tree: u.Tree): Option[(u.Tree, u.Tree)]
    }

    /**
     * Contains objects that can be used to model comprehension syntax and monad operators as
     * first class citizen.
     */
    class Syntax(val monad: u.Symbol) {

      //@formatter:off
      val Monad  = monad.asType.toType.typeConstructor
      val module = Some(core.Ref(ComprehensionSyntax.module))
      //@formatter:on

      // -----------------------------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------------------------

      object Map extends MonadOp {

        override val symbol = monad.info
          .member(api.TermName("map")).asMethod

        override def apply(xs: u.Tree)(f: u.Tree): u.Tree = {
          assert(xs.tpe.typeConstructor == API.DataBag)
          core.DefCall(Some(xs), symbol, Seq(elemTpe(f)), Seq(Seq(f)))
        }

        override def unapply(apply: u.Tree): Option[(u.Tree, u.Tree)] = apply match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(Seq(f))) => Some(xs, f)
          case _ => None
        }

        @inline
        private def elemTpe(f: u.Tree): u.Type =
          api.Type.arg(2, f.tpe)
      }

      object FlatMap extends MonadOp {

        override val symbol = monad.info
          .member(api.TermName("flatMap")).asMethod

        override def apply(xs: u.Tree)(f: u.Tree): u.Tree = {
          assert(api.Type.arg(2, f.tpe).typeConstructor == API.DataBag)
          core.DefCall(Some(xs), symbol, Seq(elemTpe(f)), Seq(Seq(f)))
        }

        override def unapply(tree: u.Tree): Option[(u.Tree, u.Tree)] = tree match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(Seq(f))) => Some(xs, f)
          case _ => None
        }

        @inline
        private def elemTpe(f: u.Tree): u.Type =
          api.Type.arg(1, api.Type.arg(2, f.tpe))
      }

      object WithFilter extends MonadOp {

        override val symbol = monad.info
          .member(api.TermName("withFilter")).asMethod

        override def apply(xs: u.Tree)(p: u.Tree): u.Tree =
          core.DefCall(Some(xs), symbol, Seq.empty, Seq(Seq(p)))

        override def unapply(tree: u.Tree): Option[(u.Tree, u.Tree)] = tree match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(Seq(p))) => Some(xs, p)
          case _ => None
        }
      }

      // -----------------------------------------------------------------------
      // Mock Comprehension Ops
      // -----------------------------------------------------------------------

      /** Con- and destructs a comprehension from/to a list of qualifiers `qs` and a head expression `hd`. */
      object Comprehension {
        val symbol = ComprehensionSyntax.comprehension

        def apply(qs: Seq[u.Tree], hd: u.Tree): u.Tree = {
          val args = Seq(api.Block(qs, hd))
          core.DefCall(module, symbol, Seq(elemTpe(hd), Monad), Seq(args))
        }

        def unapply(tree: u.Tree): Option[(Seq[u.Tree], u.Tree)] = tree match {
          case core.DefCall(_, `symbol`, _, Seq(Seq(api.Block(qs, hd)))) => Some(qs, hd)
          case _ => None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          expr.tpe
      }

      /** Con- and destructs a generator from/to a tree. */
      object Generator {
        val symbol = ComprehensionSyntax.generator

        def apply(lhs: u.TermSymbol, rhs: u.Block): u.Tree =
          core.ValDef(lhs, core.DefCall(module, symbol, Seq(elemTpe(rhs), Monad), Seq(Seq(rhs))))

        def unapply(tree: u.ValDef): Option[(u.TermSymbol, u.Block)] = tree match {
          case core.ValDef(lhs, core.DefCall(_, `symbol`, _, Seq(Seq(arg: u.Block)))) =>
            Some(lhs, arg)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type.arg(1, expr.tpe)
      }

      /** Con- and destructs a guard from/to a tree. */
      object Guard {
        val symbol = ComprehensionSyntax.guard

        def apply(expr: u.Block): u.Tree =
          core.DefCall(module, symbol, Seq.empty, Seq(Seq(expr)))

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, Seq(Seq(expr: u.Block))) => Some(expr)
          case _ => None
        }
      }

      /** Con- and destructs a head from/to a tree. */
      object Head {
        val symbol = ComprehensionSyntax.head

        def apply(expr: u.Block): u.Tree =
          core.DefCall(module, symbol, Seq(elemTpe(expr)), Seq(Seq(expr)))

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, Seq(Seq(expr: u.Block))) => Some(expr)
          case _ => None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          expr.tpe
      }

      /** Con- and destructs a flatten from/to a tree. */
      object Flatten {
        val symbol = ComprehensionSyntax.flatten

        def apply(expr: u.Block): u.Tree =
          core.DefCall(module, symbol, Seq(elemTpe(expr), Monad), Seq(Seq(expr)))

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, Seq(Seq(expr: u.Block))) => Some(expr)
          case _ => None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type.arg(1, api.Type.arg(1, expr.tpe))
      }

    }

    // ---------------------------------------------------------------------------
    // Combinators
    // ---------------------------------------------------------------------------

    private[compiler] object Combinators {

      val module = Some(core.Ref(ComprehensionCombinators.module))

      object Cross {

        val symbol = ComprehensionCombinators.cross

        def apply(xs: u.Tree, ys: u.Tree): u.Tree =
          core.DefCall(module, symbol, Seq(Core.bagElemTpe(xs), Core.bagElemTpe(ys)), Seq(Seq(xs, ys)))

        def unapply(apply: u.Tree): Option[(u.Tree, u.Tree)] = apply match {
          case core.DefCall(_, `symbol`, _, Seq(Seq(xs, ys))) => Some(xs, ys)
          case _ => None
        }
      }

      object EquiJoin {

        val symbol = ComprehensionCombinators.equiJoin

        def apply(kx: u.Tree, ky: u.Tree)(xs: u.Tree, ys: u.Tree): u.Tree = {
          val keyTpe = api.Type.arg(2, kx.tpe)
          assert(keyTpe =:= api.Type.arg(2, ky.tpe))

          core.DefCall(module, symbol,
            Seq(Core.bagElemTpe(xs), Core.bagElemTpe(ys), keyTpe),
            Seq(Seq(kx, ky), Seq(xs, ys)))
        }

        def unapply(apply: u.Tree): Option[(u.Tree, u.Tree, u.Tree, u.Tree)] = apply match {
          case core.DefCall(_, `symbol`, _, Seq(Seq(kx, ky), Seq(xs, ys))) => Some(kx, ky, xs, ys)
          case _ => None
        }
      }

    }

    // -------------------------------------------------------------------------
    // ReDeSugar API
    // -------------------------------------------------------------------------

    /** Delegates to [[ReDeSugar.resugar()]]. */
    def resugar(monad: u.Symbol): u.Tree => u.Tree =
      ReDeSugar.resugar(monad)

    /** Delegates to [[ReDeSugar.desugar()]]. */
    def desugar(monad: u.Symbol): u.Tree => u.Tree =
      ReDeSugar.desugar(monad)

    // -------------------------------------------------------------------------
    // Normalize API
    // -------------------------------------------------------------------------

    /** Delegates to [[Normalize.normalize()]]. */
    def normalize(monad: u.Symbol)(tree: u.Tree): u.Tree =
      Normalize.normalize(monad)(tree)

    // -------------------------------------------------------------------------
    // Combine API
    // -------------------------------------------------------------------------

    /** Delegates to [[Combination.transform]]. */
    lazy val combine = Combination.transform

    // -------------------------------------------------------------------------
    // General helpers
    // -------------------------------------------------------------------------

    /** Wraps `tree` in a Let block if necessary. */
    private[comprehension] def asLet(tree: u.Tree): u.Block = tree match {
      case let @ core.Let(_, _, _) => let
      case _ => core.Let(expr = tree)
    }

    /** Splits a `Seq[A]` into a prefix and suffix, excluding `pivot`. */
    private[comprehension] def splitAt[A](pivot: A)(xs: Seq[A]): (Seq[A], Seq[A]) =
      xs.span(_ != pivot) match {
        case (prefix, Seq(_, suffix @ _*)) => (prefix, suffix)
        case (prefix, _) => (prefix, Seq.empty)
      }

    /** Prepends and binds free variables in `tree` to `vals`. */
    private[comprehension] def capture(
      cs: Comprehension.Syntax,
      vals: Seq[u.ValDef],
      prune: Boolean = true
    )(tree: u.Tree): u.Tree = {
      val prefix = if (!prune) vals
        else vals.filter(api.Tree.refs(tree).compose(_.symbol.asTerm))

      def prepend(let: u.Tree): u.Block = let match {
        case core.Let(suffix, defs, expr) =>
          core.Let(prefix ++ suffix, defs, expr)
        case expr =>
          core.Let(prefix, Seq.empty, expr)
      }

      tree match {
        case cs.Generator(x, gen) => cs.Generator(x, prepend(gen))
        case cs.Guard(pred) => cs.Guard(prepend(pred))
        case cs.Head(expr) => cs.Head(prepend(expr))
        case _ => prepend(tree)
      }
    }
  }
}
