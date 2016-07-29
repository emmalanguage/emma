package eu.stratosphere
package emma.compiler
package lang
package core

/** Core language lifting / lowering. Uses Direct-style lef-normal form (LNF). */
private[core] trait LNF extends Common {
  self: Core =>

  import universe._

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
    def lift(tree: Tree): Tree = {
      tree
    }

    /**
     * Lower a LNF [[Tree]] back as Emma core language [[Tree]].
     *
     * @param tree A direct-style let-nomal form [[Tree]].
     * @return A Scala [[Tree]] derived by deconstructing the IR tree.
     */
    def lower(tree: Tree): Tree = {
      tree
    }
  }
}
