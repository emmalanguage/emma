package eu.stratosphere.emma.macros.program.ir

import _root_.eu.stratosphere.emma.macros.program.ContextHolder

import _root_.scala.reflect.macros.blackbox.Context

/**
 * Compile-time mirror of eu.stratosphere.emma.mc.* used in the macro implementations.
 */
trait IntermediateRepresentation[C <: Context] extends ContextHolder[C] {

  import c.universe._

  case class ExpressionRoot(var expr: Expression)

  abstract class Expression() {
  }

  abstract class MonadExpression() extends Expression {
  }

  case class MonadJoin(var expr: MonadExpression) extends MonadExpression {
  }

  case class MonadUnit(var expr: MonadExpression) extends MonadExpression {
  }

  abstract class Qualifier extends Expression {
  }

  case class Filter(var expr: Expression) extends Qualifier {
  }

  abstract class Generator(val lhs: TermName) extends Qualifier {
  }

  case class ScalaExprGenerator(override val lhs: TermName, var rhs: ScalaExpr) extends Generator(lhs) {
  }

  case class ComprehensionGenerator(override val lhs: TermName, var rhs: MonadExpression) extends Generator(lhs) {
  }

  case class ScalaExpr(var env: List[ValDef], var tree: Tree) extends Expression {
  }

  case class Comprehension(var monad: Tree, var head: Expression, var qualifiers: List[Qualifier]) extends MonadExpression {
  }

  def sequence(root: Expression): List[Expression] = root match {
    case MonadUnit(expr) => sequence(expr) ++ List(root)
    case MonadJoin(expr) => sequence(expr) ++ List(root)
    case Filter(expr) => sequence(expr) ++ List(root)
    case ScalaExprGenerator(_, rhs) => sequence(rhs) ++ List(root)
    case ComprehensionGenerator(_, rhs: MonadExpression) => sequence(rhs) ++ List(root)
    case ScalaExpr(_, _) => List(root)
    case Comprehension(_, head, qualifiers) => qualifiers.flatMap(q => sequence(q)) ++ sequence(head) ++ List(root)
  }
}
