package eu.stratosphere.emma.macros.program.dataflow

import eu.stratosphere.emma.macros.program.ContextHolder

import scala.reflect.macros.blackbox.Context

/**
 * Mirror of eu.stratosphere.emma.ir.* used in the macro implementations.
 */
trait IntermediateRepresentation[C <: Context] extends ContextHolder[C] {

  import c.universe._

  case class ExpressionRoot(var expr: Expression)

  trait Expression {
  }

  trait MonadExpression extends Expression {
  }

  case class MonadJoin(var expr: MonadExpression) extends MonadExpression {
  }

  case class MonadUnit(var expr: MonadExpression) extends MonadExpression {
  }

  trait Qualifier extends Expression {
  }

  case class Filter(var expr: Expression) extends Qualifier {
  }

  trait Generator extends Qualifier {
    var lhs: TermName
  }

  case class ScalaExprGenerator(var lhs: TermName, var rhs: ScalaExpr) extends Generator {
  }

  case class ComprehensionGenerator(var lhs: TermName, var rhs: MonadExpression) extends Generator {
  }

  case class ScalaExpr(var env: List[ValDef], var tree: Tree) extends Expression {
  }

  case class Comprehension(var monad: Tree, var head: Expression, var qualifiers: List[Qualifier]) extends MonadExpression {
  }

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  /**
   * Construct a list containing the nodes of an IR tree using depth-first, bottom-up traversal.
   *
   * @param root The root of the traversed tree.
   * @return
   */
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
