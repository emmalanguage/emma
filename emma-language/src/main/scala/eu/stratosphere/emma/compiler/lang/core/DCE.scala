package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common

/** Dead code elimination (DCE) for the Core language. */
private[core] trait DCE extends Common {
  self: Core =>

  import universe._
  import Tree._

  private[core] object DCE {

    /**
     * Eliminates unused valdefs (dead code) from a [[Tree]].
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[ANF.transform]]).
     *
     * == Postconditions ==
     * - All unused valdefs are pruned.
     *
     * @param tree The [[Tree]] to be pruned.
     * @return A [[Tree]] with the same semantics but without unused valdefs.
     */
    def dce(tree: Tree): Tree = {

      // create a mutable local copy of uses to keep track of unused terms
      val meta = new Core.Meta(tree)
      val uses = collection.mutable.Map() ++ meta.uses

      // initialize iteration variables
      var done = false
      var rslt = tree

      val decrementUses: Tree => Unit = traverse {
        case id@Ident(_) if uses.contains(id.symbol) =>
          uses(id.symbol) -= 1
          done = false
      }

      val transform = new postWalk(true) {

        import ComprehensionSyntax.comprehension

        override def template = {
          // Skip comprehension blocks
          case Block(stats, expr) if !comprehensionChild() =>
            block(
              stats filter {
                case vd@val_(sym, rhs, _) if uses.getOrElse(sym, 0) < 1 =>
                  decrementUses(rhs)
                  false
                case _ =>
                  true
              },
              expr)
        }

        /** Check if the current tree is a child fo a `comprehension` application. */
        private def comprehensionChild(): Boolean = ancestors.headOption.exists {
          case Method.call(_, `comprehension`, _, _) => true
          case _ => false
        }
      }

      while (!done) {
        done = true
        val t = transform(rslt)
        rslt = t
      }

      rslt
    }
  }

}
