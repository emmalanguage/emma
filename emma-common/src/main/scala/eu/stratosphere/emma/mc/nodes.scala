package eu.stratosphere.emma.mc

import scala.reflect.runtime.universe._

final class Dataflow(val name: String, val sinks: List[Comprehension[AlgExists, Boolean, Boolean]]) {
  def print() = for (sink <- sinks) Expression.printHelper(sink)
}

object Dataflow {
  def apply(name: String, sinks: List[Comprehension[AlgExists, Boolean, Boolean]]): Dataflow = {
    new Dataflow(name, sinks)
  }
}

abstract class Expression() {
}

object Expression {
  val ident = "        "

  def printHelper(e: Any, offset: String = ""): Unit = e match {
    case Comprehension(h, qs) => print("{| "); printHelper(h, offset + " " * 3); println(" | " ); printHelper(qs, offset + ident); print("\n" + offset + "|}")
    case ComprehensionGenerator(lhs, rhs) => print(lhs); print(" ← "); printHelper(rhs, offset + "   " + " " * lhs.length)
    case DirectGenerator(lhs, rhs) => print(lhs); print(" ← "); print(rhs.tree)
    case Nil => println("")
    case (q: Qualifier) :: Nil => print(offset); printHelper(q, offset)
    case (q: Qualifier) :: qs => print(offset); printHelper(q, offset); println(", "); printHelper(qs, offset)
    case Head(expr) => print(expr.tree)
    case Filter(expr) => print(expr.tree)
    case _ => print("〈unknown expression〉")
  }
}

abstract class Qualifier() extends Expression {
}

abstract class Generator[E, TE](val lhs: String) extends Qualifier {
}

final case class ComprehensionGenerator[CT <: Algebra[E, TE], E, TE](override val lhs: String, rhs: Comprehension[CT, E, TE]) extends Generator[E, TE](lhs) {
}

final case class DirectGenerator[E, TE](override val lhs: String, rhs: Expr[TE]) extends Generator[E, TE](lhs) {
}

final case class Filter[+X](var expr: Expr[Any]) extends Qualifier {
}

final case class Head[+X, +Y](var expr: Expr[Any]) extends Expression {
}

final case class Comprehension[CT <: Algebra[E, TE], E, TE](head: Head[Any, Any], qualifiers: List[Qualifier]) extends Expression {
}

object Comprehension {
  def apply[CT <: Algebra[E, TE], E, TE](head: Head[Any, Any], qualifiers: Qualifier*): Comprehension[CT, E, TE] = {
    new Comprehension(head, qualifiers.toList)
  }
}