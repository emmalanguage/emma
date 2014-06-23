package eu.stratosphere.emma

/**
 * Nodes for building an intermediate representation of an Emma programs.
 */
package object ir {

  import scala.reflect.runtime.universe._

  // ---------------------------------------------------
  // Base traits and classes.
  // ---------------------------------------------------

  final class Workflow(val name: String, val sinks: List[Comprehension]) {
  }

  object Workflow {
    def apply(name: String, sinks: List[Comprehension]): Workflow = new Workflow(name, sinks)
  }

  case class ExpressionRoot(var expr: Expression)

  trait Expression {
  }

  // ---------------------------------------------------
  // Monad comprehensions.
  // ---------------------------------------------------

  trait MonadExpression extends Expression {
  }

  final case class MonadJoin(var expr: Expression) extends MonadExpression {
  }

  final case class MonadUnit(var expr: Expression) extends MonadExpression {
  }

  trait Qualifier extends Expression {
  }

  final case class Filter(var expr: Expression) extends Qualifier {
  }

  trait Generator extends Qualifier {
    var lhs: String
  }

  final case class ScalaExprGenerator(var lhs: String, var rhs: ScalaExpr) extends Generator {
  }

  final case class ComprehensionGenerator(var lhs: String, var rhs: MonadExpression) extends Generator {
  }

  final case class ScalaExpr(var freeVars: List[String], var expr: Expr[Any]) extends Expression {
  }

  final case class Comprehension(var tpe: monad.Monad[Any], var head: Expression, var qualifiers: List[Qualifier]) extends MonadExpression {
  }

  // ---------------------------------------------------
  // Combinators.
  // ---------------------------------------------------

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

  /**
   * Pretty-print an IR tree.
   *
   * @param root The root of the printed tree.
   * @return
   */
  def prettyprint(root: Expression, debug: Boolean = false): Unit = {
    import scala.Predef.{print => p, println => pln}

    val ident = "        "

    def printHelper(e: Any, offset: String = ""): Unit = e match {
      case MonadJoin(expr) => p("join("); printHelper(expr, offset + "     "); p(")")
      case MonadUnit(expr) => p("unit("); printHelper(expr, offset + "     "); p(")")
      case Comprehension(t, h, qs) => p("[ "); printHelper(h, offset + " " * 2); pln(" | "); printHelper(qs, offset + ident); p("\n" + offset + "]^" + t.name + "")
      case ComprehensionGenerator(lhs, rhs) => p(lhs); p(" ← "); printHelper(rhs, offset + "   " + " " * lhs.length)
      case ScalaExprGenerator(lhs, rhs) => p(lhs); p(" ← "); p(rhs.expr.tree)
      case Filter(expr) => printHelper(expr)
      case Nil => pln("")
      case (q: Qualifier) :: Nil => p(offset); printHelper(q, offset)
      case (q: Qualifier) :: qs => p(offset); printHelper(q, offset); pln(", "); printHelper(qs, offset)
      case ScalaExpr(freeVars, expr) => p(expr.tree); if (debug) p(" <" + freeVars + "> ")
      case _ => p("〈unknown expression〉")
    }

    printHelper(root)
    pln("")
  }

  /**
   * Pretty-print a workflow.
   *
   * @param df The workflow to print.
   * @return
   */
  def prettyprint(df: Workflow): Unit = for (sink <- df.sinks) prettyprint(sink)
}
