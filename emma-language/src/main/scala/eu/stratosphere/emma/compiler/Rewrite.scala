package eu.stratosphere.emma.compiler

import eu.stratosphere.emma.ast.AST

import scala.annotation.tailrec

/** Common rule-based rewrite logic. */
trait Rewrite extends AST {

  trait Rule {
    /**
     * Fire the [[Rule]] once, using the first possible match.
     *
     * @param root The [[Tree]] to transform.
     * @return Optionally, the transformed [[u.Tree]] or [[None]] if no match was found.
     */
    def apply(root: u.Tree): Option[u.Tree]
  }

  object Engine {

    /** Post-walk a tree and exhaustively apply the given sequence of [[Rule]] objects at each subtree. */
    def bottomUp(rules: Seq[Rule]): u.Tree => u.Tree = tree => api.BottomUp.transform {
      case t => exhaust(rules)(t)
    }(tree).tree

    /** Exhaustively apply the given set of [[Rule]] objects at each subtree. */
    @tailrec
    def exhaust(rules: Seq[Rule])(tree: u.Tree): u.Tree = {
      // get the result of the first possible rule application
      val rsltOpt = (for {
        rule <- rules.view
        rslt <- rule(tree)
      } yield rslt).headOption

      rsltOpt match {
        case Some(rslt) =>
          exhaust(rules)(rslt) // recurse on success
        case None =>
          tree // terminate if no match is possible
      }
    }
  }

}
