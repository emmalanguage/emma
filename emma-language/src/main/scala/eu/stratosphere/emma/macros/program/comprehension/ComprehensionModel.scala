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

  trait Expression {
  }

  // Monads

  trait MonadExpression extends Expression {
  }

  case class MonadJoin(var expr: MonadExpression) extends MonadExpression {
  }

  case class MonadUnit(var expr: MonadExpression) extends MonadExpression {
  }

  case class Comprehension(var tpt: Tree, var head: Expression, var qualifiers: List[Qualifier]) extends MonadExpression {
  }

  // Qualifiers

  trait Qualifier extends Expression {
  }

  case class Filter(var expr: Expression) extends Qualifier {
  }

  case class Generator(var lhs: TermName, var rhs: Expression) extends Qualifier {
  }

  // Environment & Host Language Connectors

  case class ScalaExpr(var env: List[ValDef], var tree: Tree) extends Expression {
  }

  case class Read(tpt: Tree, location: Tree, format: Tree) extends Expression {
  }

  case class Write(location: Tree, format: Tree, in: Expression) extends Expression {
  }

  // Logical Operators

  case class Group(var key: ScalaExpr, var in: Expression) extends Expression {
  }

  case class FoldGroup(var tpt: Tree, var key: ScalaExpr, var in: Expression) extends Expression {
  }

  case class Fold(var tpt: Tree, var empty: ScalaExpr, var sng: ScalaExpr, var union: ScalaExpr, var in: Expression) extends Expression {
  }

  case class Distinct(var in: Expression) extends Expression {
  }

  case class Union(var l: Expression, var r: Expression) extends Expression {
  }

  case class Diff(var l: Expression, var r: Expression) extends Expression {
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

  /** Construct a list containing the nodes of a local comprehension tree using depth-first, bottom-up traversal.
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

  /** Construct a list containing the nodes of an IR tree using depth-first, bottom-up traversal.
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
    // TODO: add other operators
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
      case Comprehension(t, h, qs) => p("[ "); printHelper(h, offset + " " * 2); pln(" | "); printHelper(qs, offset + ident); p("\n" + offset + "]^" + printType(t) + "")
      // Qualifiers
      case Generator(lhs, rhs) => p(lhs); p(" ← "); printHelper(rhs, offset + "   " + " " * lhs.encodedName.toString.length)
      case Filter(expr) => printHelper(expr)
      // Environment & Host Language Connectors
      case ScalaExpr(freeVars, expr) => p(expr); if (debug) p(" <" + freeVars + "> ")
      case Read(_, location, format) => p("read("); p(location); p(")")
      case Write(location, format, in) => p("write("); p(location); p(")("); printHelper(in, offset + ident); p(")")
      // Logical Operators
      case Group(key, in) => p("group("); printHelper(key); p(")("); printHelper(in); p(")")
      case Fold(_, empty, sng, union, in) => p("fold(〈"); printHelper(empty); p(", "); printHelper(sng); p(", "); printHelper(union); p("〉, "); printHelper(in); p(")")
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
