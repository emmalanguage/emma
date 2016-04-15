package eu.stratosphere.emma.compiler.ir

import eu.stratosphere.emma.compiler.ReflectUtil

import scala.annotation.tailrec

/** Common rule-based rewrite logic. */
trait Rewrite extends ReflectUtil {

  import universe._

  trait Rule {

    /** The type of the rule match, usually a case class marking the relevant [[Tree]] parts. */
    type RuleMatch

    /** A binding function that produces syntactically viable [[RuleMatch]] objects. */
    protected def bind(root: Tree): Traversable[RuleMatch]

    /** A guarding condition that semantically validates the [[RuleMatch]]. */
    protected def guard(rm: RuleMatch): Boolean

    /** The rewrite logic. */
    protected def fire(rm: RuleMatch): Tree

    /**
     * Fire the [[Rule]] once, using the first [[RuleMatch]] that satisfies the [[guard]].
     *
     * @param root The [[Tree]] to transform.
     * @return Optionally, the transformed [[Tree]].
     */
    final def apply(root: Tree): Option[Tree] =
      for (rm <- bind(root).find(guard)) yield fire(rm)
  }

  object Engine {

    /** Post-walk a tree and exhaustively apply the given sequence of [[Rule]] objects at each subtree. */
    def postWalk(rules: Seq[Rule]): Tree => Tree = Rewrite.this.postWalk {
      case t => exhaust(rules)(t)
    }

    /** Exhaustively apply the given set of [[Rule]] objects at each subtree. */
    @tailrec
    def exhaust(rules: Seq[Rule])(tree: Tree): Tree = {
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
