package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common

/** Core language lifting / lowering. Uses Direct-style lef-normal form (LNF). */
private[core] trait LNF extends Common {
  self: Core =>

  private[core] object LNF {

    /**
     * Lift a Scala Source language [[Tree]] into let-normal form.
     *
     * This includes:
     *
     * - bringing the original tree to administrative normal form;
     * - modeling control flow as direct-style;
     * - inlining Emma API expressions.
     *
     * @param tree The core language [[Tree]] to be lifted.
     * @return A direct-style let-normal form variant of the input [[Tree]].
     */
    def lift(tree: u.Tree): u.Tree = {
      tree
    }

    /**
     * Lower a LNF [[Tree]] back as Emma core language [[Tree]].
     *
     * @param tree A direct-style let-nomal form [[Tree]].
     * @return A Scala [[Tree]] derived by deconstructing the IR tree.
     */
    def lower(tree: u.Tree): u.Tree = {
      tree
    }
  }
}
