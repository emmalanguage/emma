package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.ContextHolder

import scala.collection.mutable
import scala.reflect.macros.blackbox

/** Model and helper functions for the intermediate representation of comprehended terms.
  */
private[emma] trait ComprehensionModel[C <: blackbox.Context] extends ContextHolder[C] {

  import c.universe._

  // --------------------------------------------------------------------------
  // Comprehension Model
  // --------------------------------------------------------------------------

  case class ExpressionRoot(var expr: Expression) {
    override def toString = prettyprint(expr)
  }

  sealed trait Expression {
    def tpe: Type
  }

  // Monads

  sealed trait MonadExpression extends Expression {
  }

  case class MonadJoin(var expr: MonadExpression) extends MonadExpression {
    def tpe = expr.tpe
  }

  case class MonadUnit(var expr: MonadExpression) extends MonadExpression {
    def tpe = expr.tpe
  }

  case class Comprehension(var tpe: Type, var head: Expression, var qualifiers: List[Qualifier]) extends MonadExpression {
  }

  // Qualifiers

  sealed trait Qualifier extends Expression {
  }

  case class Filter(var expr: Expression) extends Qualifier {
    def tpe = c.typeOf[Boolean]
  }

  case class Generator(var lhs: TermName, var rhs: Expression) extends Qualifier {
    def tpe = rhs.tpe
  }

  // Environment & Host Language Connectors

  case class ScalaExpr(var env: List[ValDef], var tree: Tree) extends Expression {
    def tpe = tree.tpe.dealias
  }

  case class Read(tpe: Type, location: Tree, format: Tree) extends Expression {
  }

  case class Write(location: Tree, format: Tree, in: Expression) extends Expression {
    def tpe = in.tpe
  }

  // Logical Operators

  case class Group(var key: ScalaExpr, var in: Expression) extends Expression {
    def tpe = q"Group[${key.tpe}, DataBag[${in.tpe}]]".tpe
  }

  case class Fold(var tpe: Type, var empty: ScalaExpr, var sng: ScalaExpr, var union: ScalaExpr, var in: Expression) extends Expression {
  }

  case class Distinct(var in: Expression) extends Expression {
    def tpe = in.tpe
  }

  case class Union(var l: Expression, var r: Expression) extends Expression {
    def tpe = l.tpe
  }

  case class Diff(var l: Expression, var r: Expression) extends Expression {
    def tpe = l.tpe
  }

  // --------------------------------------------------------------------------
  // Comprehension Store
  // --------------------------------------------------------------------------

  class ComprehensionStore(val terms: mutable.Seq[ComprehendedTerm]) {

    private val defIndex = mutable.Map((for (t <- terms; d <- t.definition) yield d -> t): _*)

    private val termIndex = mutable.Map((for (t <- terms) yield t.term -> t): _*)

    def comprehendedDef(defintion: Tree) = defIndex.get(defintion)

    def getByTerm(term: Tree) = termIndex.get(term)
  }

  case class ComprehendedTerm(id: TermName, term: Tree, comprehension: ExpressionRoot, definition: Option[Tree])

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
    case Comprehension(_, head, qualifiers) => qualifiers.flatMap(q => localSeq(q)) ++ localSeq(head) ++ List(root)
    // Qualifiers
    case Filter(expr) => localSeq(expr) ++ List(root)
    case Generator(_, rhs) => localSeq(rhs) ++ List(root)
    // Environment & Host Language Connectors, Logical Operators: Skip!
    case _ => List(root)
  }

  /**
   * Construct a list containing the nodes of an IR tree using depth-first, bottom-up traversal.
   *
   * @param root The root of the traversed tree.
   * @return
   */
  def globalSeq(root: Expression): List[Expression] = root match {
    // Monads
    case MonadUnit(expr) => globalSeq(expr) ++ List(root)
    case MonadJoin(expr) => globalSeq(expr) ++ List(root)
    case Comprehension(_, head, qualifiers) => qualifiers.flatMap(q => globalSeq(q)) ++ globalSeq(head) ++ List(root)
    // Qualifiers
    case Filter(expr) => globalSeq(expr) ++ List(root)
    case Generator(_, rhs) => globalSeq(rhs) ++ List(root)
    // Environment & Host Language Connectors
    case ScalaExpr(_, _) => List(root)
    case Read(_, _, _) => List(root)
    case Write(_, _, in) => globalSeq(in) ++ List(root)
    // Logical Operators: Skip!
    case Group(key, in) => globalSeq(in) ++ globalSeq(key) ++ List(root)
    case Fold(_, empty, sng, union, in) => globalSeq(in) ++ globalSeq(empty) ++ globalSeq(sng) ++ globalSeq(union) ++ List(root)
    case Distinct(in) => globalSeq(in) ++ List(root)
    case Union(l, r) => globalSeq(l) ++ globalSeq(r) ++ List(root)
    case Diff(l, r) => globalSeq(l) ++ globalSeq(r) ++ List(root)
  }

  /**
   * Pretty-print an IR tree.
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
      case Comprehension(t, h, qs) => p("[ "); printHelper(h, offset + " " * 2); pln(" | "); printHelper(qs, offset + ident); p("\n" + offset + "]^Bag[" + t.toString + "]")
      // Qualifiers
      case Filter(expr) => printHelper(expr)
      case Generator(lhs, rhs) => p(lhs); p(" ← "); printHelper(rhs, offset + "   " + " " * lhs.encodedName.toString.length)
      // Environment & Host Language Connectors
      case ScalaExpr(freeVars, expr) => p(expr); if (debug) p(" <" + freeVars + "> ")
      case Read(_, location, format) => p("read("); p(location); p(")")
      case Write(location, format, in) => p("write("); p(location); p(")("); printHelper(in, offset + ident); p(")")
      // Logical Operators
      case Group(key, in) => p("group("); printHelper(key); p(")("); printHelper(in); p(")")
      case Fold(_, empty, sng, union, in) => p("fold(〈"); printHelper(empty); p(", "); printHelper(sng); p(", "); printHelper(union); p("〉, "); printHelper(in); p(")")
      case Distinct(in) => p("distinct("); printHelper(in); p(")");
      case Union(l, r) => p("union("); printHelper(l); p(")("); printHelper(r); p(")")
      case Diff(l, r) => p("diff("); printHelper(l); p(")("); printHelper(r); p(")")
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
