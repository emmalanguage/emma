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

  case class Variable(name: TermName, tpt: Tree) {
    def tpe = c.typecheck(tpt, c.TYPEmode).tpe
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
    def tpe = c.typecheck(tq"DataBag[${expr.tpe}]", c.TYPEmode).tpe
  }

  case class Comprehension(var head: Expression, var qualifiers: List[Qualifier]) extends MonadExpression {
    def tpe = c.typecheck(tq"DataBag[${head.tpe}]", c.TYPEmode).tpe
  }

  // Qualifiers

  sealed trait Qualifier extends Expression {
  }

  case class Filter(var expr: Expression) extends Qualifier {
    def tpe = c.typeOf[Boolean]
  }

  case class Generator(var lhs: TermName, var rhs: Expression) extends Qualifier {
    def tpe = rhs.tpe.typeArgs.head
  }

  // Environment & Host Language Connectors

  case class ScalaExpr(var vars: List[Variable], var tree: Tree) extends Expression {
    def tpe = tree.tpe
  }

  // Combinators

  object combinator {
    sealed trait Combinator extends Expression {
    }

    case class Read(location: Tree, format: Tree) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${format.tpe.typeArgs.head})]", c.TYPEmode).tpe
    }

    case class Write(location: Tree, format: Tree, in: Expression) extends Combinator {
      def tpe = typeOf[Unit]
    }

    case class Map(f: Tree, xs: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${f.tpe.typeArgs.reverse.head})]", c.TYPEmode).tpe
    }

    case class FlatMap(f: Tree, xs: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${f.tpe.typeArgs.reverse.head})]", c.TYPEmode).tpe
    }

    case class Filter(var p: Tree, var xs: Expression) extends Combinator {
      def tpe = xs.tpe
    }

    case class EquiJoin(p: Tree, xs: Expression, ys: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${xs.tpe.typeArgs.head}, ${ys.tpe.typeArgs.head})]", c.TYPEmode).tpe
    }

    case class Cross(xs: Expression, ys: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${xs.tpe.typeArgs.head}, ${ys.tpe.typeArgs.head})]", c.TYPEmode).tpe
    }

    case class Group(var key: Tree, var xs: Expression) extends Combinator {
      def tpe = c.typecheck(tq"Group[${key.tpe.typeArgs.tail.head}, DataBag[${xs.tpe.typeArgs.head}]]", c.TYPEmode).tpe
    }

    case class Fold(var empty: Tree, var sng: Tree, var union: Tree, var xs: Expression) extends Combinator {
      def tpe = sng.tpe.typeArgs.tail.head // Function[A, B]#B
    }

    case class FoldGroup(var key: Tree, var empty: Tree, var sng: Tree, var union: Tree, var xs: Expression) extends Combinator {
      def tpe = sng.tpe.typeArgs.tail.head // Function[A, B]#B
    }

    case class Distinct(var xs: Expression) extends Combinator {
      def tpe = xs.tpe
    }

    case class Union(var xs: Expression, var ys: Expression) extends Combinator {
      def tpe = xs.tpe
    }

    case class Diff(var xs: Expression, var ys: Expression) extends Combinator {
      def tpe = xs.tpe
    }

    case class TempSource(ident: Ident) extends Combinator {
      val tpe = ident.symbol.info
    }
  }

  // --------------------------------------------------------------------------
  // Traversal
  // --------------------------------------------------------------------------

  trait ExpressionTransformer {

    def transform(e: Expression): Expression = e match {
      // Monads
      case MonadUnit(in) => MonadUnit(transformMonadExpression(in))
      case MonadJoin(in) => MonadJoin(transformMonadExpression(in))
      case Comprehension(head, qualifiers) => Comprehension(transform(head), transformQualifiers(qualifiers))
      // Qualifiers
      case Filter(in) => Filter(transform(in))
      case Generator(lhs, rhs) => Generator(lhs, transform(rhs))
      // Environment & Host Language Connectors
      case ScalaExpr(vars, tree) => ScalaExpr(vars, tree)
      // Combinators
      case combinator.Read(location, format) => combinator.Read(location, format)
      case combinator.Write(location, format, in) => combinator.Write(location, format, transform(in))
      case combinator.Map(f, xs) => combinator.Map(f, transform(xs))
      case combinator.FlatMap(f, xs) => combinator.FlatMap(f, transform(xs))
      case combinator.Filter(p, xs) => combinator.Filter(p, transform(xs))
      case combinator.EquiJoin(p, xs, ys) => combinator.EquiJoin(p, transform(xs), transform(ys))
      case combinator.Cross(xs, ys) => combinator.Cross(transform(xs), transform(ys))
      case combinator.Group(key, xs) => combinator.Group(key, transform(xs))
      case combinator.Fold(empty, sng, union, xs) => combinator.Fold(empty, sng, union, transform(xs))
      case combinator.FoldGroup(key, empty, sng, union, xs) => combinator.FoldGroup(key, empty, sng, union, transform(xs))
      case combinator.Distinct(xs) => combinator.Distinct(transform(xs))
      case combinator.Union(xs, ys) => combinator.Union(transform(xs), transform(ys))
      case combinator.Diff(xs, ys) => combinator.Diff(transform(xs), transform(ys))
      case combinator.TempSource(ident) => combinator.TempSource(ident)
    }

    def transformMonadExpression(e: MonadExpression) = transform(e).asInstanceOf[MonadExpression]

    def transformQualifiers(qs: List[Qualifier]) = for (q <- qs) yield transformQualifier(q)

    def transformQualifier(e: Qualifier) = transform(e).asInstanceOf[Qualifier]

    def transformScalaExpr(e: ScalaExpr) = transform(e).asInstanceOf[ScalaExpr]
  }

  trait ExpressionDepthFirstTraverser {

    def traverse(e: Expression): Unit = e match {
      // Monads
      case MonadUnit(expr) => traverse(expr)
      case MonadJoin(expr) => traverse(expr)
      case Comprehension(head, qualifiers) => qualifiers foreach traverse; traverse(head)
      // Qualifiers
      case Filter(expr) => traverse(expr)
      case Generator(_, rhs) => traverse(rhs)
      // Environment & Host Language Connectors
      case ScalaExpr(_, _) => Unit
      // Combinators
      case combinator.Read(_, _) => Unit
      case combinator.Write(_, _, xs) => traverse(xs)
      case combinator.Map(f, xs) => traverse(xs)
      case combinator.FlatMap(f, xs) => traverse(xs)
      case combinator.Filter(p, xs) => traverse(xs)
      case combinator.EquiJoin(p, xs, ys) => traverse(xs); traverse(xs)
      case combinator.Cross(xs, ys) => traverse(xs); traverse(xs)
      case combinator.Group(_, xs) => traverse(xs)
      case combinator.Fold(_, _, _, xs) => traverse(xs)
      case combinator.FoldGroup(key, empty, sng, union, xs) => traverse(xs)
      case combinator.Distinct(xs) => traverse(xs)
      case combinator.Union(xs, ys) => traverse(xs); traverse(ys)
      case combinator.Diff(xs, ys) => traverse(xs); traverse(ys)
      case combinator.TempSource(ident) => Unit
    }
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
    case Comprehension(head, qualifiers) => qualifiers.flatMap(q => localSeq(q)) ++ localSeq(head) ++ List(root)
    // Qualifiers
    case Filter(expr) => localSeq(expr) ++ List(root)
    case Generator(_, rhs) => localSeq(rhs) ++ List(root)
    // Environment & Host Language Connectors, Combinators: Skip!
    case _ => List(root)
  }

  /**
   * Pretty-print an IR tree.
   *
   * @param root The root of the printed tree.
   * @return
   */
  def prettyprint(root: Expression, debug: Boolean = false) = {
    val sb = new mutable.StringBuilder()
    val ident = "        "

    def print(v: Any) = v match {
      case x: TermName => sb.append(x.encodedName.toString)
      case x: Tree => sb.append(x.toString())
      case x: String => sb.append(x)
      case _ => sb.append("<unknown>")
    }

    def println(v: String) = sb.append(v + sys.props("line.separator"))

    def printHelper(e: Any, offset: String = ""): Unit = e match {
      // Monads
      case e@MonadJoin(expr) => print("join("); printHelper(expr, offset + ident); print(")")
      case e@MonadUnit(expr) => print("unit("); printHelper(expr, offset + ident); print(")")
      case e@Comprehension(h, qs) => print("[ "); printHelper(h, offset + " " * 2); println(" | "); printHelper(qs, offset + ident); print("\n" + offset + "]^" + e.tpe.toString)
      // Qualifiers
      case e@Filter(expr) => printHelper(expr)
      case e@Generator(lhs, rhs) => print(lhs); print(" ← "); printHelper(rhs, offset + "   " + " " * lhs.encodedName.toString.length)
      // Environment & Host Language Connectors
      case e@ScalaExpr(freeVars, expr) => print(expr); if (debug) print(" <" + freeVars + "> ")
      // Combinators
      case e@combinator.Read(location, format) => print("read ("); print(location); print(")")
      case e@combinator.Write(location, format, in) => print("write ("); print(location); print(")("); printHelper(in, offset + ident); print(")")
      case e@combinator.Map(f, xs) => print("map "); print(f); print("("); printHelper(xs); print(")")
      case e@combinator.FlatMap(f, xs) => print("flatMap "); print(f); print("("); printHelper(xs); print(")")
      case e@combinator.Filter(p, xs) => print("filter "); print(p); print("("); printHelper(xs); print(")")
      case e@combinator.EquiJoin(p, xs, ys) => print("join "); print(p); print("("); printHelper(xs); print(")")
      case e@combinator.Cross(xs, ys) => print("cross "); print("("); printHelper(xs); print(")")
      case e@combinator.Group(key, xs) => print("group "); print(key); print("("); printHelper(xs); print(")")
      case e@combinator.Fold(empty, sng, union, xs) => print("fold〈"); print(empty); print(", "); print(sng); print(", "); print(union); print("〉("); printHelper(xs); print(")")
      case e@combinator.Distinct(xs) => print("distinct "); printHelper(xs); print(")");
      case e@combinator.Union(xs, ys) => print("union "); printHelper(xs); print(")("); printHelper(ys); print(")")
      case e@combinator.Diff(xs, ys) => print("diff "); printHelper(xs); print(")("); printHelper(ys); print(")")
      case e@combinator.TempSource(id) => print("temp ("); print(id); print(")")
      // Lists
      case Nil => println("")
      case q :: Nil => print(offset); printHelper(q, offset)
      case q :: qs => print(offset); printHelper(q, offset); println(", "); printHelper(qs, offset)
      case _ => print("〈unknown expression〉")
    }

    printHelper(root)
    sb.toString()
  }
}
