package eu.stratosphere.emma.rewrite

trait RewriteEngine {

  type Expression

  abstract class Rule {

    type RuleMatch

    protected def bind(expr: Expression, root: Expression): Traversable[RuleMatch]

    protected def guard(rm: RuleMatch): Boolean

    protected def fire(rm: RuleMatch): Expression

    final def apply(expr: Expression, root: Expression): Option[Expression] = {
      for (rm <- bind(expr, root).find(guard)) yield fire(rm)
    }
  }

}
