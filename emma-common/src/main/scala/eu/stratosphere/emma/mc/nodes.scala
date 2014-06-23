package eu.stratosphere.emma.mc

import scala.reflect.runtime.universe._

final class Dataflow(val name: String, val sinks: List[Comprehension]) {
  def print() = for (sink <- sinks) {
    Expression.printHelper(sink); println("")
  }
}

object Dataflow {
  def apply(name: String, sinks: List[Comprehension]): Dataflow = {
    new Dataflow(name, sinks)
  }
}

trait Expression {
}

object Expression {
  val ident = "        "

  def printHelper(e: Any, offset: String = ""): Unit = e match {
    case MonadJoin(expr) => print("join("); printHelper(expr, offset + "     "); print(")")
    case MonadUnit(expr) => print("unit("); printHelper(expr, offset + "     "); print(")")
    case Comprehension(t, h, qs) => print("[ "); printHelper(h, offset + " " * 2); println(" | "); printHelper(qs, offset + ident); print("\n" + offset + "]^" + t.name + "")
    case ComprehensionGenerator(lhs, rhs) => print(lhs); print(" ← "); printHelper(rhs, offset + "   " + " " * lhs.length)
    case ScalaExprGenerator(lhs, rhs) => print(lhs); print(" ← "); print(rhs.expr.tree)
    case Filter(expr) => printHelper(expr)
    case Nil => println("")
    case (q: Qualifier) :: Nil => print(offset); printHelper(q, offset)
    case (q: Qualifier) :: qs => print(offset); printHelper(q, offset); println(", "); printHelper(qs, offset)
    case ScalaExpr(freeVars, expr) => print(expr.tree); print(" <" + freeVars + "> ")
    case _ => print("〈unknown expression〉")
  }
}

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

final case class ScalaExpr(var env: List[String], var expr: Expr[Any]) extends Expression {
}

final case class Comprehension(var tpe: monad.Monad[Any], var head: Expression, var qualifiers: List[Qualifier]) extends MonadExpression {
}

object Comprehension {
  def apply(tpe: monad.Monad[Any], head: Expression, qualifiers: Qualifier*): Comprehension = {
    new Comprehension(tpe, head, qualifiers.toList)
  }
}