package eu.stratosphere.emma.rewrite

trait RewriteEngine {

  type ExpressionRoot

  val rules: List[Rule]

  def rewrite(root: ExpressionRoot): ExpressionRoot = {
    while ((for (r <- rules) yield r.apply(root)).fold(false)(_ || _)) {} // apply rules while possible
    root
  }

  abstract class Rule {

    type RuleMatch

    protected def bind(r: ExpressionRoot): Traversable[RuleMatch]

    protected def guard(r: ExpressionRoot, m: RuleMatch): Boolean

    protected def fire(r: ExpressionRoot, m: RuleMatch): Unit

    final def apply(e: ExpressionRoot) =
      (for (m <- bind(e)) yield // for each match
        if (guard(e, m)) // fire rule and return true if guard passes
          (fire(e, m) -> true)._2
        else // don't fire rule and return false otherwise
          false).fold(false)(_ || _) // compute if fired rule exists
  }

}
