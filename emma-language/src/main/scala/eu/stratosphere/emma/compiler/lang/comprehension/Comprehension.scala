package eu.stratosphere.emma
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
      val monadTpe  = monad.asType.toType.typeConstructor
      val moduleSel = api.Tree.resolveStatic(ComprehensionSyntax.module)
      //@formatter:on

      // -----------------------------------------------------------------------
      // Monad Ops
      // -----------------------------------------------------------------------

      object Map extends MonadOp {

        override val symbol =
          api.Term.member(monad, api.TermName("map")).asMethod // API: access method directly

        override def apply(xs: u.Tree)(f: u.Tree): u.Tree = {
          assert(xs.tpe.typeConstructor == API.DATA_BAG)
          core.DefCall(Some(xs))(symbol, elemTpe(f))(f :: Nil)
        }

        override def unapply(apply: u.Tree): Option[(u.Tree, u.Tree)] = apply match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(f)) => Some(xs, f)
          case _ => None
        }

        @inline
        private def elemTpe(f: u.Tree): u.Type =
          api.Type.arg(2, api.Type.of(f))
      }

      object FlatMap extends MonadOp {

        override val symbol =
          api.Term.member(monad, api.TermName("flatMap")).asMethod // API: access method directly

        override def apply(xs: u.Tree)(f: u.Tree): u.Tree = {
          assert(api.Type.arg(2, f.tpe).typeConstructor == API.DATA_BAG)
          core.DefCall(Some(xs))(symbol, elemTpe(f))(f :: Nil)
        }

        override def unapply(tree: u.Tree): Option[(u.Tree, u.Tree)] = tree match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(f)) => Some(xs, f)
          case _ => None
        }

        @inline
        private def elemTpe(f: u.Tree): u.Type =
          api.Type.arg(1, api.Type.arg(2, api.Type.of(f)))
      }

      object WithFilter extends MonadOp {

        override val symbol =
          api.Term.member(monad, api.TermName("withFilter")).asMethod // API: access method directly

        override def apply(xs: u.Tree)(p: u.Tree): u.Tree =
          core.DefCall(Some(xs))(symbol)(p :: Nil)

        override def unapply(tree: u.Tree): Option[(u.Tree, u.Tree)] = tree match {
          case core.DefCall(Some(xs), `symbol`, _, Seq(p)) => Some(xs, p)
          case _ => None
        }
      }

      // -----------------------------------------------------------------------
      // Mock Comprehension Ops
      // -----------------------------------------------------------------------

      /** Con- and destructs a comprehension from/to a list of qualifiers `qs` and a head expression `hd`. */
      object Comprehension {
        val symbol = ComprehensionSyntax.comprehension

        def apply(qs: Seq[u.Tree], hd: u.Tree): u.Tree =
          core.DefCall(Some(moduleSel))(symbol, elemTpe(hd), monadTpe)(api.Block(qs:_*)(hd) :: Nil)

        def unapply(tree: u.Tree): Option[(Seq[u.Tree], u.Tree)] = tree match {
          case core.DefCall(_, `symbol`, _, api.Block(qs, hd) :: Nil) =>
            Some(qs, hd)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type of expr
      }

      /** Con- and destructs a generator from/to a [[Tree]]. */
      object Generator {
        val symbol = ComprehensionSyntax.generator

        def apply(lhs: u.TermSymbol, rhs: u.Block): u.Tree = core.ValDef(
          lhs,
          core.DefCall(Some(moduleSel))(symbol, elemTpe(rhs), monadTpe)(rhs :: Nil))

        def unapply(tree: u.ValDef): Option[(u.TermSymbol, u.Block)] = tree match {
          case core.ValDef(lhs, core.DefCall(_, `symbol`, _, (arg: u.Block) :: Nil), _) =>
            Some(lhs, arg)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type.arg(1, api.Type of expr)
      }

      /** Con- and destructs a guard from/to a [[Tree]]. */
      object Guard {
        val symbol = ComprehensionSyntax.guard

        def apply(expr: u.Block): u.Tree =
          core.DefCall(Some(moduleSel))(symbol)(expr :: Nil)

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, (expr: u.Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }
      }

      /** Con- and destructs a head from/to a [[Tree]]. */
      object Head {
        val symbol = ComprehensionSyntax.head

        def apply(expr: u.Block): u.Tree =
          core.DefCall(Some(moduleSel))(symbol, elemTpe(expr))(expr :: Nil)

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, (expr: u.Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type of expr
      }

      /** Con- and destructs a flatten from/to a [[Tree]]. */
      object Flatten {
        val symbol = ComprehensionSyntax.flatten

        def apply(expr: u.Block): u.Tree =
          core.DefCall(Some(moduleSel))(symbol, elemTpe(expr), monadTpe)(expr :: Nil)

        def unapply(tree: u.Tree): Option[u.Block] = tree match {
          case core.DefCall(_, `symbol`, _, (expr: u.Block) :: Nil) =>
            Some(expr)
          case _ =>
            None
        }

        @inline
        private def elemTpe(expr: u.Tree): u.Type =
          api.Type.arg(1, api.Type.arg(1, api.Type of expr))
      }

    }

    // ---------------------------------------------------------------------------
    // Combinators
    // ---------------------------------------------------------------------------

    private[compiler] object Combinators {

      val moduleSel = api.Tree.resolveStatic(ComprehensionCombinators.module)

      object Cross {

        val symbol = ComprehensionCombinators.cross

        def apply(xs: u.Tree, ys: u.Tree): u.Tree =
          core.DefCall(Some(moduleSel))(symbol, Core.bagElemTpe(xs), Core.bagElemTpe(ys))(Seq(xs, ys))

        def unapply(apply: u.Tree): Option[(u.Tree, u.Tree)] = apply match {
          case core.DefCall(Some(`moduleSel`), `symbol`, _, Seq(xs, ys)) => Some(xs, ys)
          case _ => None
        }
      }

      object EquiJoin {

        val symbol = ComprehensionCombinators.equiJoin

        def apply(kx: u.Tree, ky: u.Tree)(xs: u.Tree, ys: u.Tree): u.Tree = {
          val keyTpe = api.Type.arg(2, kx.tpe)
          assert(keyTpe == api.Type.arg(2, ky.tpe)) // See comment before maybeAddCast in Combination

          core.DefCall(Some(moduleSel))(
            symbol, Core.bagElemTpe(xs), Core.bagElemTpe(ys), keyTpe
          )(
            Seq(kx, ky), Seq(xs, ys)
          )
        }

        def unapply(apply: u.Tree): Option[(u.Tree, u.Tree, u.Tree, u.Tree)] = apply match {
          case core.DefCall(Some(`moduleSel`), `symbol`, _, Seq(kx, ky), Seq(xs, ys)) => Some(kx, ky, xs, ys)
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

    private[comprehension] def asLet(tree: u.Tree): u.Block = tree match {
      case let @ core.Let(_, _, _) => let
      case other => core.Let()()(other)
    }

    /* Splits a `Seq[A]` into a prefix and suffix. */
    private[comprehension] def splitAt[A](e: A): Seq[A] => (Seq[A], Seq[A]) = {
      (_: Seq[A]).span(_ != e)
    } andThen {
      case (pre, Seq(_, suf@_*)) => (pre, suf)
    }
  }

}
