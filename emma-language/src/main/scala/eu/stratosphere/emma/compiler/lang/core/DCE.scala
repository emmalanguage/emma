package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common

/** Dead code elimination (DCE) for the Core language. */
private[core] trait DCE extends Common {
  self: Core =>

  import UniverseImplicits._
  import Core.{Lang => core}

  private[core] object DCE {

    /**
     * Eliminates unused value definitions (dead code) from a tree.
     *
     * == Preconditions ==
     * - The input tree is in LNF (see [[Core.lnf]]).
     *
     * == Postconditions ==
     * - All unused value definitions are pruned.
     */
    lazy val transform: u.Tree => u.Tree =
      api.BottomUp.withValUses.transformWithSyn {
        case Attr(let @ core.Let(vals, defs, expr), _, _, syn) =>
          def refs(tree: u.Tree) = syn(tree).head.keySet
          val liveRefs = vals.foldRight(refs(expr)) {
            case (core.ValDef(lhs, rhs, _), live) =>
              if (live(lhs)) live | refs(rhs) else live
          }

          val liveVals = vals.filter(liveRefs compose api.TermSym.of)
          if (liveVals.size == vals.size) let
          else core.Let(liveVals: _*)(defs: _*)(expr)
      }.andThen(_.tree)
  }
}
