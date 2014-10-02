package eu.stratosphere.emma

import eu.stratosphere.emma.api.{OutputFormat, InputFormat}

import scala.collection.mutable

/**
 * Nodes for building an intermediate representation of an Emma programs.
 */
package object ir {

  import scala.reflect.runtime.universe._
  import scala.language.existentials // TODO: required for Read[+A], see why

  // ---------------------------------------------------
  // Thunks.
  // ---------------------------------------------------

  trait Env {
    def get[A](name: String): A

    //    def put[A](value: DataSet[A]): A
    //
    //    def execute
  }

  def force[A](thunk: Thunk[A])(implicit env: Env) = env.get(thunk.name)

  final class Thunk[A](val name: String) {
  }

  // --------------------------------------------------------------------------
  // Comprehension Model
  // --------------------------------------------------------------------------

  case class ExpressionRoot(var expr: Expression) {
    override def toString = prettyprint(expr)
  }

  sealed trait Expression {
  }

  // Monads

  sealed trait MonadExpression extends Expression {
  }

  final case class MonadJoin[+A](var expr: MonadExpression)(implicit val tag: TypeTag[_ <: A]) extends MonadExpression {
  }

  final case class MonadUnit[+A](var expr: MonadExpression)(implicit val tag: TypeTag[_ <: A]) extends MonadExpression {
  }

  final case class Comprehension[+A](var head: Expression, var qualifiers: List[Qualifier])(implicit val tag: TypeTag[_ <: A]) extends MonadExpression {
  }

  // Qualifiers

  sealed trait Qualifier extends Expression {
  }

  final case class Filter(var expr: Expression) extends Qualifier {
  }

  // TODO: try using a single generator like the one defined below instead of the generator family
  // case class Generator(var lhs: String, var rhs: Expression) extends Qualifier {
  // }

  sealed trait Generator extends Qualifier {
    var lhs: String
  }

  final case class ScalaExprGenerator[+A](var lhs: String, var rhs: ScalaExpr[Any])(implicit val tag: TypeTag[_ <: A]) extends Generator {
  }

  final case class ComprehensionGenerator[+A](var lhs: String, var rhs: Expression)(implicit val tag: TypeTag[_ <: A]) extends Generator {
  }

  // Environment & Host Language Connectors

  // TODO: rename freeVars to env
  case class ScalaExpr[+A](var freeVars: Map[String, Expr[Any]], var expr: Expr[Any])(implicit val tag: TypeTag[_ <: A])  extends Expression {
  }

  case class Read[+A](location: String, format: InputFormat[_ <: A])(implicit val tag: TypeTag[_ <: A]) extends Expression {
  }

  case class Write[+A](location: String, format: OutputFormat[_ <: A], in: Expression)(implicit val tag: TypeTag[_ <: A]) extends Expression {
  }

  // Logical Operators

  case class Group[+A](var key: ScalaExpr[Any], var in: Expression)(implicit val tag: TypeTag[_ <: A]) extends Expression {
  }

  case class Fold[+A](var empty: ScalaExpr[Any], var sng: ScalaExpr[Any], var union: ScalaExpr[Any], var in: Expression)(implicit val tag: TypeTag[_ <: A]) extends Expression {
  }

  case class Distinct[+A](var in: Expression)(implicit val tag: TypeTag[_ <: A]) extends Expression {
  }

  case class Union[+A](var l: Expression, var r: Expression)(implicit val tag: TypeTag[_ <: A]) extends Expression {
  }

  case class Diff[+A](var l: Expression, var r: Expression)(implicit val tag: TypeTag[_ <: A]) extends Expression {
  }

  // ---------------------------------------------------
  // Combinators.
  // ---------------------------------------------------

  case class FilterCombinator(p: Filter, xs: Generator) extends Generator {
    var lhs = xs.lhs

    override def toString = "Filter"
  }

  case class EquiJoinCombinator(var lhs: String, p: Filter, xs: Generator, ys: Generator) extends Generator {
    override def toString = s"Join(lhs = $lhs)"
  }

  case class ThetaJoinCombinator(var lhs: String, p: Filter, xs: Generator, ys: Generator) extends Generator {
    override def toString = s"Join(lhs = $lhs)"
  }

  case class CrossCombinator(var lhs: String, qs: List[Generator]) extends Generator {
    override def toString = s"Cross(lhs = $lhs)"
  }

  case class MapCombinator(var body: ScalaExpr[Any], var input: Qualifier) extends MonadExpression {
    override def toString = s"Map"
  }

  case class FlatMapCombinator(var body: ScalaExpr[Any], var input: Qualifier) extends MonadExpression {
    override def toString = s"FlatMap"
  }

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  /**
   * Construct a list containing the nodes of a local comprehension tree using depth-first, bottom-up traversal.
   *
   * This method will trim subtrees with nodes which represent
   *
   * - Logical Operators or
   * - Environment & Host Language Connectors.
   *
   * @param root The root of the traversed tree.
   * @return
   */
  def localSeq(root: Expression): List[Expression] = root match {
    // Monads
    case MonadUnit(expr) => localSeq(expr) ++ List(root)
    case MonadJoin(expr) => localSeq(expr) ++ List(root)
    case Comprehension(head, qualifiers) => qualifiers.flatMap(q => localSeq(q)) ++ localSeq(head) ++ List(root)
    // Qualifiers
    case Filter(expr) => localSeq(expr) ++ List(root)
    case ScalaExprGenerator(_, rhs) => localSeq(rhs) ++ List(root)
    case ComprehensionGenerator(_, rhs) => localSeq(rhs) ++ List(root)
    // Environment & Host Language Connectors, Logical Operators: Skip!
    case _ => List(root)
  }

  /** Construct a list containing the nodes of an IR tree using depth-first, bottom-up traversal.
    *
    * @param root The root of the traversed tree.
    * @return
    */
  def globalSeq(root: Expression): List[Expression] = root match {
    // Monads
    case MonadUnit(expr) => globalSeq(expr) ++ List(root)
    case MonadJoin(expr) => globalSeq(expr) ++ List(root)
    case Comprehension(head, qualifiers) => qualifiers.flatMap(q => globalSeq(q)) ++ globalSeq(head) ++ List(root)
    // Qualifiers
    case Filter(expr) => globalSeq(expr) ++ List(root)
    case ScalaExprGenerator(_, rhs) => localSeq(rhs) ++ List(root)
    case ComprehensionGenerator(_, rhs) => localSeq(rhs) ++ List(root)
    // Environment & Host Language Connectors
    case ScalaExpr(_, _) => List(root)
    case Read(_, _) => List(root)
    case Write(_, _, in) => globalSeq(in) ++ List(root)
    // Logical Operators: Skip!
    case Group(key, in) => globalSeq(in) ++ globalSeq(key) ++ List(root)
    case Fold(empty, sng, union, in) => globalSeq(in) ++ globalSeq(empty) ++ globalSeq(sng) ++ globalSeq(union) ++ List(root)
    case Distinct(in) => globalSeq(in) ++ List(root)
    case Union(l, r) => globalSeq(l) ++ globalSeq(r) ++ List(root)
    case Diff(l, r) => globalSeq(l) ++ globalSeq(r) ++ List(root)
    // Combinators
    case FilterCombinator(p, xs) => globalSeq(xs) ++ globalSeq(p) ++ List(root)
    case EquiJoinCombinator(_, p, xs, ys) => globalSeq(ys) ++ globalSeq(xs) ++ globalSeq(p) ++ List(root)
    case ThetaJoinCombinator(_, p, xs, ys) => globalSeq(ys) ++ globalSeq(xs) ++ globalSeq(p) ++ List(root)
    case CrossCombinator(_, qs) => qs.flatMap(q => globalSeq(q)) ++ List(root)
    case MapCombinator(e, qs) => globalSeq(qs) ++ List(root)
  }

  /** Pretty-print an IR tree.
    *
    * @param root The root of the printed tree.
    * @return
    */
  def prettyprint(root: Expression, debug: Boolean = false) = {
    val sb = new mutable.StringBuilder()

    def p(v: Any) = v match {
      case x: TermName => sb.append(x.encodedName.toString)
      case x: Tree => sb.append(x.toString())
      case x: String => sb.append(x)
      case _ => sb.append("<unknown>")
    }

    def pln(v: String) = sb.append(v + sys.props("line.separator"))

    def printType(tpe: Tree) = tpe.toString().stripPrefix("eu.stratosphere.emma.api.")

    val ident = "        "

    def printHelper(e: Any, offset: String = ""): Unit = e match {
      // Monads
      case MonadJoin(expr) => p("join("); printHelper(expr, offset + "     "); p(")")
      case MonadUnit(expr) => p("unit("); printHelper(expr, offset + "     "); p(")")
      case Comprehension(h, qs) => p("[ "); printHelper(h, offset + " " * 2); pln(" | "); printHelper(qs, offset + ident); p("\n" + offset + "]^" /*+ printType(t)*/ + "") // TODO: print
      // Qualifiers
      case ComprehensionGenerator(lhs, rhs) => p(lhs); p(" ← "); printHelper(rhs, offset + "   " + " " * lhs.length)
      case ScalaExprGenerator(lhs, rhs) => p(lhs); p(" ← "); printHelper(rhs, offset + "   " + " " * lhs.length)
      case Filter(expr) => printHelper(expr)
      // Environment & Host Language Connectors
      case ScalaExpr(freeVars, expr) => p(expr); if (debug) p(" <" + freeVars + "> ")
      case Read(location, format) => p("read("); p(location); p(")")
      case Write(location, format, in) => p("write("); p(location); p(")("); printHelper(in, offset + ident); p(")")
      // Logical Operators
      case Group(key, in) => p("group("); printHelper(key); p(")("); printHelper(in); p(")")
      case Fold(empty, sng, union, in) => p("fold(〈"); printHelper(empty); p(", "); printHelper(sng); p(", "); printHelper(union); p("〉, "); printHelper(in); p(")")
      case Distinct(in) => p("distinct("); printHelper(in); p(")");
      case Union(l, r) => p("union("); printHelper(l); p(")("); printHelper(r); p(")")
      case Diff(l, r) => p("diff("); printHelper(l); p(")("); printHelper(r); p(")")
      // Combinators
      case EquiJoinCombinator(l, p, x, y) => print(s"$l ← ("); printHelper(p); print(s")(${x.lhs}) ⋈ "); print(s"(${y.lhs})")
      case ThetaJoinCombinator(l, p, x, y) => print(s"$l ← ("); printHelper(p); print(s")(${x.lhs}) ⋈ "); print(s"(${y.lhs})")
      case CrossCombinator(l, qs) => print(s"$l ← "); val b = StringBuilder.newBuilder; qs.map(q => q.lhs).addString(b, "(", ") ⨯ (", ")"); print(b.result())
      case FilterCombinator(p, xs) => print(s"${xs.lhs} ← σ ("); printHelper(p); print(")("); printHelper(xs); print(")")
      case MapCombinator(e, qs) => print("map ("); printHelper(e); print(")("); printHelper(qs); print(")")
      // Lists
      case Nil => pln("")
      case q :: Nil => p(offset); printHelper(q, offset)
      case q :: qs => p(offset); printHelper(q, offset); pln(", "); printHelper(qs, offset)
      case _ => p("〈unknown expression〉")
    }

    printHelper(root)
    sb.toString()
  }

}
