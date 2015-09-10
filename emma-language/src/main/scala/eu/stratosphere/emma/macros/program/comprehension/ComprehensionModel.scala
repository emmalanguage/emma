package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.BlackBoxUtil
import scala.collection.mutable

/**
 * Model and helper functions for the intermediate representation of comprehended terms.
 */
private[emma] trait ComprehensionModel extends BlackBoxUtil {
  import universe._

  // --------------------------------------------------------------------------
  // Comprehension Model
  // --------------------------------------------------------------------------

  case class ExpressionRoot(var expr: Expression) {

    override def toString = prettyprint(expr)

    /**
     * Extract the `comprehension` closure, i.e. all [[TermSymbol]]s that are defined outside of
     * the comprehended [[Tree]].
     *
     * As per passing convention adhered by the environment implementations, the computed closure
     * is returned as a list lexicographically ordered by the [[Symbol]]s' [[TermName]]s.
     *
     * @return The closure as a list lexicographically ordered by the [[Symbol]]s' [[TermName]]s
     */
    lazy val freeTerms: List[TermSymbol] = {
      val c = combinator
      expr.collect { // Combinators
        case c.Map(f, _)                    => f.freeTerms
        case c.FlatMap(f, _)                => f.freeTerms
        case c.Filter(p, _)                 => p.freeTerms
        case c.EquiJoin(kx, ky, _, _)       => kx.freeTerms union ky.freeTerms
        case c.Group(k, _)                  => k.freeTerms
        case c.Fold(em, sg, un, _, _)       => Set(em, sg, un)    flatMap { _.freeTerms }
        case c.FoldGroup(k, em, sg, un, _)  => Set(k, em, sg, un) flatMap { _.freeTerms }
        case c.TempSource(id) if id.hasTerm => Set(id.term)
      }.flatten.distinct sortBy { _.fullName }
    }
  }

  case class Variable(name: TermName, tpt: Tree) {
    def tpe = c.typecheck(tpt, c.TYPEmode).tpe.widen
  }

  sealed trait Expression {

    def tpe: Type

    def elementType: Type =
      tpe.elementType

    /** Apply `f` to each subtree */
    def sequence() = collect({
      case x => x
    })

    /** Apply `pf` to each subexpression on which the function is defined and collect the results. */
    def collect[T](pf: PartialFunction[Expression, T]): List[T] = {
      val ctt = new CollectTreeTraverser[T](pf)
      ctt.traverse(this)
      ctt.results.toList
    }

    override def toString = prettyprint(this)
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

  case class Generator(lhs: TermName, var rhs: Expression) extends Qualifier {
    def tpe = rhs.tpe.typeArgs.head
  }

  // Environment & Host Language Connectors

  case class ScalaExpr(var vars: List[Variable], var tree: Tree) extends Expression {
    def tpe = tree.tpe.widen

    /** Restrict the `vars` to the ones referenced in the expression `tree`. */
    def usedVars = {
      // collecte the termnames
      val termNames = tree.collect({
        case Ident(t: TermName) => t
      }).toSet[TermName]
      vars.filter(x => termNames.contains(x.name))
    }

    /**
     * Simultaneously substitutes all free occurrences of `key` with `value` in this expression
     * and adapts its environment.
     *
     * @param key The old node to be substituted
     * @param value The new node to be substituted in place of the old one
     */
    def substitute(key: TermName, value: ScalaExpr): Unit = {
      // remove key from vars
      vars = vars.filter { _.name != key } ++ (value.vars diff vars)
      // add all additional value.vars
      tree = tree.substitute(key, value.tree).typeChecked
    }
  }

  // Combinators

  object combinator {

    sealed trait Combinator extends Expression {
    }

    case class Read(location: Tree, format: Tree) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${format.tpe.widen.typeArgs.head})]", c.TYPEmode).tpe
    }

    case class Write(location: Tree, format: Tree, in: Expression) extends Combinator {
      def tpe = typeOf[Unit]
    }

    case class TempSource(id: Ident) extends Combinator {
      val tpe = id.symbol.info
    }

    case class TempSink(id: TermName, var xs: Expression) extends Combinator {
      val tpe = xs.tpe
    }

    case class Map(f: Tree, xs: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${f.tpe.widen.typeArgs.reverse.head})]", c.TYPEmode).tpe
    }

    case class FlatMap(f: Tree, xs: Expression) extends Combinator {
      def tpe = f.tpe.widen.typeArgs.reverse.head // Since the return type of is DataBag[DataBag[T]], don't wrap!
    }

    case class Filter(var p: Tree, var xs: Expression) extends Combinator {
      def tpe = xs.tpe
    }

    case class EquiJoin(keyx: Tree, keyy: Tree, xs: Expression, ys: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${xs.tpe.typeArgs.head}, ${ys.tpe.typeArgs.head})]", c.TYPEmode).tpe
    }

    case class Cross(xs: Expression, ys: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[(${xs.tpe.typeArgs.head}, ${ys.tpe.typeArgs.head})]", c.TYPEmode).tpe
    }

    case class Group(var key: Tree, var xs: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[Group[${key.tpe.widen.typeArgs.tail.head}, DataBag[${xs.tpe.typeArgs.head}]]]", c.TYPEmode).tpe
    }

    case class Fold(var empty: Tree, var sng: Tree, var union: Tree, var xs: Expression, var origin: Tree) extends Combinator {
      def tpe = sng.tpe.widen.typeArgs.tail.head // Function[A, B]#B
    }

    case class FoldGroup(var key: Tree, var empty: Tree, var sng: Tree, var union: Tree, var xs: Expression) extends Combinator {
      def tpe = c.typecheck(tq"DataBag[Group[${key.tpe.widen.typeArgs.tail.head}, ${sng.tpe.widen.typeArgs.tail.head}]]", c.TYPEmode).tpe
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

  }

  // --------------------------------------------------------------------------
  // Comprehension Store
  // --------------------------------------------------------------------------

  class ComprehensionView(ctx: mutable.Seq[ComprehendedTerm]) {

    private val defIndex = mutable.Map((for (t <- ctx; d <- t.definition) yield d -> t): _*)

    private val termIndex = mutable.Map((for (t <- ctx) yield t.term -> t): _*)

    def terms = termIndex.values.toStream

    def comprehendedDef(defintion: Tree) = defIndex.get(defintion)

    def getByTerm(term: Tree) = termIndex.get(term)

    def remove(t: ComprehendedTerm) = {
      for (d <- t.definition) defIndex -= d // remove from defIndex
      termIndex -= t.term // remove from termIndex
    }
  }

  case class ComprehendedTerm(id: TermName, term: Tree, comprehension: ExpressionRoot, definition: Option[Tree])

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
      case combinator.TempSource(id) => combinator.TempSource(id)
      case combinator.TempSink(id, xs) => combinator.TempSink(id, transform(xs))
      case combinator.Map(f, xs) => combinator.Map(f, transform(xs))
      case combinator.FlatMap(f, xs) => combinator.FlatMap(f, transform(xs))
      case combinator.Filter(p, xs) => combinator.Filter(p, transform(xs))
      case combinator.EquiJoin(keyx, keyy, xs, ys) => combinator.EquiJoin(keyx, keyy, transform(xs), transform(ys))
      case combinator.Cross(xs, ys) => combinator.Cross(transform(xs), transform(ys))
      case combinator.Group(key, xs) => combinator.Group(key, transform(xs))
      case combinator.Fold(empty, sng, union, xs, origin) => combinator.Fold(empty, sng, union, transform(xs), origin)
      case combinator.FoldGroup(key, empty, sng, union, xs) => combinator.FoldGroup(key, empty, sng, union, transform(xs))
      case combinator.Distinct(xs) => combinator.Distinct(transform(xs))
      case combinator.Union(xs, ys) => combinator.Union(transform(xs), transform(ys))
      case combinator.Diff(xs, ys) => combinator.Diff(transform(xs), transform(ys))
    }

    def transformMonadExpression(e: MonadExpression) = transform(e).asInstanceOf[MonadExpression]

    def transformQualifiers(qs: List[Qualifier]) = for (q <- qs) yield transformQualifier(q)

    def transformQualifier(e: Qualifier) = transform(e).asInstanceOf[Qualifier]

    def transformScalaExpr(e: ScalaExpr) = transform(e).asInstanceOf[ScalaExpr]

    def transformFold(e: combinator.Fold) = transform(e).asInstanceOf[combinator.Fold]
  }

  trait ExpressionTraverser {

    def traverse(e: Expression): Unit = e match {
      // Monads
      case MonadUnit(expr) => traverse(expr)
      case MonadJoin(expr) => traverse(expr)
      case Comprehension(head, qualifiers) => qualifiers foreach traverse; traverse(head)
      // Qualifiers
      case Filter(expr) => traverse(expr)
      case Generator(_, rhs) => traverse(rhs)
      // Environment & Host Language Connectors
      case ScalaExpr(_, _) =>
      // Combinators
      case combinator.Read(_, _) =>
      case combinator.Write(_, _, xs) => traverse(xs)
      case combinator.TempSource(_) =>
      case combinator.TempSink(_, xs) => traverse(xs)
      case combinator.Map(_, xs) => traverse(xs)
      case combinator.FlatMap(_, xs) => traverse(xs)
      case combinator.Filter(_, xs) => traverse(xs)
      case combinator.EquiJoin(_, _, xs, ys) => traverse(xs); traverse(ys)
      case combinator.Cross(xs, ys) => traverse(xs); traverse(ys)
      case combinator.Group(_, xs) => traverse(xs)
      case combinator.Fold(_, _, _, xs, _) => traverse(xs)
      case combinator.FoldGroup(_, _, _, _, xs) => traverse(xs)
      case combinator.Distinct(xs) => traverse(xs)
      case combinator.Union(xs, ys) => traverse(xs); traverse(ys)
      case combinator.Diff(xs, ys) => traverse(xs); traverse(ys)
    }
  }

  private class CollectTreeTraverser[T](pf: PartialFunction[Expression, T]) extends ExpressionTraverser {
    val results = mutable.ListBuffer[T]()

    override def traverse(t: Expression) {
      super.traverse(t)
      if (pf.isDefinedAt(t)) results += pf(t)
    }
  }

  // --------------------------------------------------------------------------
  // Helper methods.
  // --------------------------------------------------------------------------

  /**
   * Typechecks a `tree` using the given set of environtment `vars`.
   */
  def typechecked(vars: List[Variable], tree: Tree) =
    if (tree.tpe == null)
      c.typecheck(q"{ ..${for (v <- vars.reverse) yield q"val ${v.name}: ${v.tpt} = null.asInstanceOf[${v.tpt}]".asInstanceOf[ValDef]}; $tree }").asInstanceOf[Block].expr
    else
      tree

  /**
   * Pretty-print an IR tree.
   *
   * @param root The root of the printed tree.
   * @return
   */
  def prettyprint(root: Expression, debug: Boolean = false) = {
    def pp(e: Any, offset: String = ""): String = e match {
      // Monads
      case e@MonadJoin(expr) =>
        s"""
           |μ ( ${pp(expr, offset + " " * 4)} )
        """.stripMargin.trim
      case e@MonadUnit(expr) =>
        s"""
           |η ( ${pp(expr, offset + " " * 4)} )
        """.stripMargin.trim
      case e@Comprehension(h, qs) =>
        s"""
           |[ ${pp(h, offset + " " * 2)}|
                                        | ${offset + pp(qs, offset + " " * 2)} ]
        """.stripMargin.trim //^${e.tpe.toString}
      // Qualifiers
      case e@Filter(expr) =>
        s"""
           |${pp(expr)}
        """.stripMargin.trim
      case e@Generator(lhs, rhs) =>
        s"""
           |$lhs ← ${pp(rhs, offset + "   " + " " * lhs.encodedName.toString.length)}
        """.stripMargin.trim
      // Environment & Host Language Connectors
      case e@ScalaExpr(freeVars, expr) =>
        s"""
           |${pp(expr)}
        """.stripMargin.trim // if (debug) _pprint(" <" + freeVars + "> ")
      // Combinators
      case e@combinator.Read(location, format) =>
        s"""
           |read (${pp(location)})
        """.stripMargin.trim
      case e@combinator.Write(location, format, in) =>
        s"""
           |write (${pp(location)}) (
                                    |       ${offset + pp(in, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.TempSource(id) =>
        s"""
           |tmpsrc (${pp(id)})
        """.stripMargin.trim
      case e@combinator.TempSink(id, xs) =>
        s"""
           |tmpsnk ${pp(id)} (
                              |       ${offset + pp(xs, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.Map(f, xs) =>
        s"""
           |map ${pp(f)} (
                          |       ${offset + pp(xs, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.FlatMap(f, xs) =>
        s"""
           |flatMap ${pp(f)} (
                              |       ${offset + pp(xs, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.Filter(p, xs) =>
        s"""
           |filter ${pp(p)} (
                             |       ${offset + pp(xs, offset + " " * 8)})
        """.stripMargin.trim
      case e@combinator.EquiJoin(keyx, keyy, xs, ys) =>
        s"""
           |join ${pp(keyx)} ${pp(keyy)} (
                                          |       ${offset + pp(xs, offset + " " * 8)} ,
                                                                                        |       ${offset + pp(ys, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.Cross(xs, ys) =>
        s"""
           |cross (
           |       ${offset + pp(xs, offset + " " * 8)} ,
                                                         |       ${offset + pp(ys, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.Group(key, xs) =>
        s"""
           |group ${pp(key)} (
                              |       ${offset + pp(xs, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.FoldGroup(key, empty, sng, union, xs) =>
        s"""
           |foldGroup〈${pp(empty)}, ${pp(sng)}, ${pp(union)}〉 ${pp(key)} (
                                                                          |       ${offset + pp(xs, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.Fold(empty, sng, union, xs, _) =>
        s"""
           |fold〈${pp(empty)}, ${pp(sng)}, ${pp(union)}〉(
                                                         |       ${offset + pp(xs, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.Distinct(xs) =>
        s"""
           |distinct (${pp(xs)})
                                 |       ${offset + pp(xs, offset)})
        """.stripMargin.trim
      case e@combinator.Union(xs, ys) =>
        s"""
           |union (
           |       ${offset + pp(xs, offset + " " * 8)} ,
                                                         |       ${offset + pp(ys, offset + " " * 8)} )
        """.stripMargin.trim
      case e@combinator.Diff(xs, ys) =>
        s"""
           |diff (
           |       ${offset + pp(xs, offset + " " * 8)} ,
                                                         |       ${offset + pp(ys, offset + " " * 8)} )
        """.stripMargin.trim
      // Lists
      case qs: List[Any] =>
        (for (q <- qs) yield pp(q, offset)).mkString(sys.props("line.separator") + offset)
      // Other types
      case x: TermName =>
        x.encodedName.toString
      case x: Tree =>
        x.toString()
      case x: String =>
        x
      case _ =>
        "〈unknown expression〉"
    }

    pp(root)
  }
}
