package eu.stratosphere.emma.macros.program.comprehension.rewrite

import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel
import eu.stratosphere.emma.rewrite.RewriteEngine

trait ComprehensionRewriteEngine extends ComprehensionModel with RewriteEngine {

  /**
   * Construct a rule application function that breaks after one successful application.
   *
   * @param rules A sequence of rules to be applied
   * @return A one-off rule application function
   */
  protected def applyAny(rules: Rule*): ExpressionRoot => Boolean =
    new ExpressionTransformer with (ExpressionRoot => Boolean) {
      
      var changed = false
  
      override def transform(expr: Expression) = {
        if (!changed) applyFirst(rules: _*)(expr) match {
          case Some(x) => changed = true; x
          case None    => super.transform(expr)
        } else expr
      }
  
      /**
       * Exhaustively apply the given set of `rules` to a tree and all its subtrees.
       *
       * @param root The root of the expression tree to be transformed
       * @return A [[Boolean]] indicating whether some rule was applied at all
       */
      def apply(root: ExpressionRoot) = {
        changed   = false
        root.expr = transform(root.expr)
        changed
      }
    }

  /**
   * Construct an exhaustive application function for a given set of transformation rules.
   *
   * @param rules A sequence of rules to be applied
   * @return An exhaustive rule application function
   */
  protected def applyExhaustively(rules: Rule*): ExpressionRoot => Boolean =
    new ExpressionTransformer with  (ExpressionRoot => Boolean) {
      
      var changed = false
  
      override def transform(expr: Expression) =
        applyFirst(rules: _*)(expr) match {
          case Some(x) => changed = true; x
          case None    => super.transform(expr)
        }
  
      /**
       * Exhaustively apply the given set of `rules` to a tree and all its subtrees.
       *
       * @param root The root of the expression tree to be transformed
       * @return A [[Boolean]] indicating whether some rule was applied at all
       */
      def apply(root: ExpressionRoot) = {
        var i = -1
  
        do {
          i += 1
          changed   = false
          root.expr = transform(root.expr)
        } while (changed)
  
        i > 0
      }
    }

  /**
   * Construct a break-on-success application function for a given sequence of transformation rules.
   *
   * @param rules A sequence of rules to be applied
   * @return A one-off rule application function
   */
  protected def applyFirst(rules: Rule*): Expression => Option[Expression] =
    expr => rules.foldLeft(Option.empty[Expression]) {
      case (Some(x), _) => Some(x)
      case (None, rule) => rule apply expr
    }
}
