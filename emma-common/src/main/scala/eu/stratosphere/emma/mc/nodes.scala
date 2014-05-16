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

abstract class Expression() {
}

object Expression {
  val ident = "        "

  def printHelper(e: Any, offset: String = ""): Unit = e match {
    case Comprehension(t, h, qs) => print("[ "); printHelper(h, offset + " " * 3); println(" | "); printHelper(qs, offset + ident); print("\n" + offset + "]^" + t.name + "")
    case ComprehensionGenerator(lhs, rhs) => print(lhs); print(" ← "); printHelper(rhs, offset + "   " + " " * lhs.length)
    case ScalaExprGenerator(lhs, rhs) => print(lhs); print(" ← "); print(rhs.expr.tree)
    case Filter(expr) => printHelper(expr)
    case Nil => println("")
    case (q: Qualifier) :: Nil => print(offset); printHelper(q, offset)
    case (q: Qualifier) :: qs => print(offset); printHelper(q, offset); println(", "); printHelper(qs, offset)
    case ScalaExpr(expr) => print(expr.tree)
    case _ => print("〈unknown expression〉")
  }
}

abstract class Qualifier extends Expression {
}

final case class Filter(expr: Expression) extends Qualifier {
}

abstract class Generator(val lhs: String) extends Qualifier {
}

final case class ScalaExprGenerator(override val lhs: String, rhs: ScalaExpr) extends Generator(lhs) {
}

final case class ComprehensionGenerator(override val lhs: String, rhs: Comprehension) extends Generator(lhs) {
}

final case class ScalaExpr(var expr: Expr[Any]) extends Expression {
}

final case class Comprehension(tpe: monad.Monad[Any], head: Expression, qualifiers: List[Qualifier]) extends Expression {
}

object Comprehension {
  def apply(tpe: monad.Monad[Any], head: Expression, qualifiers: Qualifier*): Comprehension = {
    new Comprehension(tpe, head, qualifiers.toList)
  }
}