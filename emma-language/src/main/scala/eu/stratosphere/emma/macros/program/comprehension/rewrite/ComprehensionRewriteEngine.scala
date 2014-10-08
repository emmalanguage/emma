package eu.stratosphere.emma.macros.program.comprehension.rewrite

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel
import eu.stratosphere.emma.rewrite.RewriteEngine

import scala.reflect.macros.blackbox

trait ComprehensionRewriteEngine[C <: blackbox.Context]
  extends ContextHolder[C]
  with ComprehensionModel[C]
  with RewriteEngine {

  /**
   * Constructs a rule application function that breaks after one successfull application.
   *
   * @param rules A sequence of rules to be applied.
   */
  protected def applyAny(rules: Rule*) = new (ExpressionRoot => Boolean) with ExpressionTransformer {
    var changed = false

    override def transform(e: Expression): Expression = {
      if (!changed) applyFirst(rules: _*)(e) match {
        case Some(x) => changed = true; x
        case _ => super.transform(e)
      } else {
        e;
      }
    }

    /**
     * Exhaustively applies the given set of `rules` to a tree and all its subtrees.
     *
     * @param root The root of the expression tree to be transformed.
     * @return A boolean indicating whether some rule was applied at all.
     */
    override def apply(root: ExpressionRoot) = {
      changed = false
      root.expr = transform(root.expr)
      changed
    }
  }

  /**
   * Constructs an exhaustive application function for a given set of transformation rules.
   *
   * @param rules A sequence of rules to be applied.
   */
  protected def applyExhaustively(rules: Rule*) = new (ExpressionRoot => Boolean) with ExpressionTransformer {
    var changed = false

    override def transform(e: Expression): Expression = applyFirst(rules: _*)(e) match {
      case Some(x) => changed = true; x
      case _ => super.transform(e)
    }

    /**
     * Exhaustively applies the given set of `rules` to a tree and all its subtrees.
     *
     * @param root The root of the expression tree to be transformed.
     * @return A boolean indicating whether some rule was applied at all.
     */
    override def apply(root: ExpressionRoot) = {
      var i = -1

      do {
        i = i + 1
        changed = false
        root.expr = transform(root.expr)
        changed
      } while (changed)

      i > 0
    }
  }

  /**
   * Construct a break-on-success application function for a given sequence of transformation rules.
   *
   * @param rules A sequence of rules to be applied.
   */
  protected def applyFirst(rules: Rule*) = (e: Expression) => rules.foldLeft(Option.empty[Expression])((x: Option[Expression], r: Rule) => x match {
    case Some(_) => x
    case _ => r.apply(e)
  })

}
