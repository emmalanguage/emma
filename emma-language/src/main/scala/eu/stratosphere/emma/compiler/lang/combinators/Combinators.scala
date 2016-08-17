package eu.stratosphere.emma
package compiler.lang.combinators

import compiler.Common
import compiler.lang.core.Core

trait Combinators extends Common {
  self: Core =>

  import UniverseImplicits._
  import Core.{Lang => core}

  private[emma] object Combinator {

    val moduleSel = api.Tree.resolveStatic(IR.module)

    object Cross {

      val symbol = IR.cross

      def apply(xs: u.Tree, ys: u.Tree): u.Tree =
        core.DefCall(Some(moduleSel))(symbol, Core.bagElemTpe(xs), Core.bagElemTpe(ys))(Seq(xs, ys))

      def unapply(apply: u.Tree): Option[(u.Tree, u.Tree)] = apply match {
        case core.DefCall(Some(`moduleSel`), `symbol`, _, Seq(xs, ys)) => Some(xs, ys)
        case _ => None
      }
    }

    object EquiJoin {

      val symbol = IR.equiJoin

      def apply(kx: u.Tree, ky: u.Tree)(xs: u.Tree, ys: u.Tree): u.Tree = {
        val keyTpe = api.Type.arg(2, kx.tpe)
        assert(keyTpe == api.Type.arg(2, ky.tpe)) // See comment before maybeAddCast in Combination

        core.DefCall(Some(moduleSel)) (
          symbol, Core.bagElemTpe(xs), Core.bagElemTpe(ys), keyTpe
        ) (
          Seq(kx, ky), Seq(xs, ys)
        )
      }

      def unapply(apply: u.Tree): Option[(u.Tree, u.Tree, u.Tree, u.Tree)] = apply match {
        case core.DefCall(Some(`moduleSel`), `symbol`, _, Seq(kx, ky), Seq(xs, ys)) => Some(kx, ky, xs, ys)
        case _ => None
      }
    }

  }
}
