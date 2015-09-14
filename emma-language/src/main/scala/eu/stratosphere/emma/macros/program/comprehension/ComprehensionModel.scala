package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.api
import eu.stratosphere.emma.macros.BlackBoxUtil
import scala.collection.mutable

/** Model and helper functions for the intermediate representation of comprehended terms. */
private[emma] trait ComprehensionModel extends BlackBoxUtil {
  import universe._

  // Type constructors
  val DATA_BAG = typeOf[api.DataBag[Nothing]].typeConstructor
  val GROUP    = typeOf[api.Group[Nothing, Nothing]].typeConstructor

  // --------------------------------------------------------------------------
  // Comprehension Model
  // --------------------------------------------------------------------------

  case class ExpressionRoot(var expr: Expression) {

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
      }.flatten.toList.distinct sortBy { _.fullName }
    }

    override def toString =
      prettyPrint(expr)
  }

  sealed trait Expression extends Traversable[Expression] {

    /** @return The [[Type]] of this [[Expression]] */
    def tpe: Type

    /** @return The element [[Type]] of this [[Expression]] */
    def elementType: Type = tpe.elementType

    /**
     * Recurse down the children of this [[Expression]] and apply a side-effecting function to each
     * one. Implementations of this method should fulfill the following criteria:
     *
     * 1. Call [[Expression.foreach]] on every child;
     * 2. DO NOT call [[Expression.descend]] explicitly;
     * 3. Recurse in a bottom-up instead of top-down order.
     *
     * @param f A side-effecting function to apply to each child
     * @tparam U The return type of the side-effecting function
     */
    protected def descend[U](f: Expression => U): Unit

    def foreach[U](f: Expression => U) = {
      descend(f); f(this)
    }

    override def toString() =
      prettyPrint(this)
  }

  // Monads

  sealed trait MonadExpression extends Expression

  case class MonadJoin(var expr: MonadExpression) extends MonadExpression {
    def tpe = expr.tpe
    def descend[U](f: Expression => U) = expr foreach f
  }

  case class MonadUnit(var expr: MonadExpression) extends MonadExpression {
    def tpe = DATA_BAG(expr.tpe)
    def descend[U](f: Expression => U) = expr foreach f
  }

  case class Comprehension(var hd: Expression, var qualifiers: List[Qualifier])
      extends MonadExpression {

    def tpe = DATA_BAG(hd.tpe)

    def descend[U](f: Expression => U) = {
      for (q <- qualifiers) q foreach f
      hd foreach f
    }
  }

  // Qualifiers

  sealed trait Qualifier extends Expression

  case class Filter(var expr: Expression) extends Qualifier {
    def tpe = typeOf[Boolean]
    def descend[U](f: Expression => U) = expr foreach f
  }

  case class Generator(lhs: TermSymbol, var rhs: Expression) extends Qualifier {
    def tpe = rhs.elementType
    def descend[U](f: Expression => U) = rhs foreach f
  }

  // Environment & Host Language Connectors

  case class ScalaExpr(var vars: List[ValDef], var tree: Tree) extends Expression {

    def tpe = tree.trueType

    def descend[U](f: Expression => U) = ()

    /** @return All `vars` referenced in the expression `tree` */
    def usedVars: List[ValDef] = {
      // collect the term names
      val names = tree.collect { case Ident(name: TermName) => name }.toSet
      for (v <- vars if names(v.name)) yield v
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
      vars = vars.filter { _.name != key } ::: (value.vars diff vars)
      // add all additional value.vars
      tree = tree.substitute(key, value.tree).typeChecked
    }
  }

  // Combinators

  object combinator {

    sealed trait Combinator extends Expression

    case class Read(location: Tree, format: Tree) extends Combinator {
      def tpe = DATA_BAG(format.elementType)
      def descend[U](f: Expression => U) = ()
    }

    case class Write(location: Tree, format: Tree, in: Expression) extends Combinator {
      def tpe = typeOf[Unit]
      def descend[U](f: Expression => U) = in foreach f
    }

    case class TempSource(id: Ident) extends Combinator {
      val tpe = id.trueType
      def descend[U](f: Expression => U) = ()
    }

    case class TempSink(id: TermName, var xs: Expression) extends Combinator {
      val tpe = xs.tpe
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class Map(f: Tree, xs: Expression) extends Combinator {
      def tpe = DATA_BAG(f.trueType.typeArgs.last)
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class FlatMap(f: Tree, xs: Expression) extends Combinator {
      // Since the return type of is DataBag[DataBag[T]], don't wrap!
      def tpe = f.trueType.typeArgs.last
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class Filter(var p: Tree, var xs: Expression) extends Combinator {
      def tpe = xs.tpe
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class EquiJoin(keyx: Tree, keyy: Tree, xs: Expression, ys: Expression)
        extends Combinator {

      def tpe = DATA_BAG(PAIR(xs.elementType, ys.elementType))

      def descend[U](f: Expression => U): Unit = {
        xs foreach f
        ys foreach f
      }
    }

    case class Cross(xs: Expression, ys: Expression) extends Combinator {

      def tpe = DATA_BAG(PAIR(xs.elementType, ys.elementType))

      def descend[U](f: Expression => U) = {
        xs foreach f
        ys foreach f
      }
    }

    case class Group(var key: Tree, var xs: Expression) extends Combinator {

      def tpe = {
        val K = key.trueType.typeArgs(1)
        val V = DATA_BAG(xs.elementType)
        DATA_BAG(GROUP(K, V))
      }

      def descend[U](f: Expression => U) =
        xs foreach f
    }

    case class Fold(
        var empty:  Tree,
        var sng:    Tree,
        var union:  Tree,
        var xs:     Expression,
        var origin: Tree) extends Combinator {

      def tpe = sng.trueType.typeArgs(1) // Function[A, B]#B

      def descend[U](f: Expression => U) = xs foreach f
    }

    case class FoldGroup(
        var key:   Tree,
        var empty: Tree,
        var sng:   Tree,
        var union: Tree,
        var xs:    Expression) extends Combinator {

      def tpe = {
        val K = key.trueType.typeArgs(1)
        val V = sng.trueType.typeArgs(1)
        DATA_BAG(GROUP(K, V))
      }

      def descend[U](f: Expression => U) =
        xs foreach f
    }

    case class Distinct(var xs: Expression) extends Combinator {
      def tpe = xs.tpe
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class Union(var xs: Expression, var ys: Expression) extends Combinator {

      def tpe = ys.tpe

      def descend[U](f: Expression => U) = {
        xs foreach f
        ys foreach f
      }
    }

    case class Diff(var xs: Expression, var ys: Expression) extends Combinator {

      def tpe = ys.tpe

      def descend[U](f: Expression => U) = {
        xs foreach f
        ys foreach f
      }
    }

  }

  // --------------------------------------------------------------------------
  // Comprehension Store
  // --------------------------------------------------------------------------

  class ComprehensionView(ctx: mutable.Seq[ComprehendedTerm]) {

    private val defIndex =
      mutable.Map((for (ct <- ctx; df <- ct.definition) yield df -> ct): _*)

    private val termIndex =
      mutable.Map((for (ct <- ctx) yield ct.term -> ct): _*)

    def terms: Stream[ComprehendedTerm] =
      termIndex.values.toStream

    def comprehendedDef(definition: Tree): Option[ComprehendedTerm] =
      defIndex get definition

    def getByTerm(term: Tree): Option[ComprehendedTerm] =
      termIndex get term

    def remove(ct: ComprehendedTerm): Unit = {
      for (df <- ct.definition) defIndex -= df // remove from defIndex
      termIndex -= ct.term // remove from termIndex
    }
  }

  case class ComprehendedTerm(
      id:            TermName,
      term:          Tree,
      comprehension: ExpressionRoot,
      definition:    Option[Tree])

  // --------------------------------------------------------------------------
  // Transformation
  // --------------------------------------------------------------------------

  /** Provides methods for top-down recursive transformation of [[Expression]]s. */
  trait ExpressionTransformer {
    val c = combinator

    /**
     * Transform an [[Expression]] in a top-down recursive way by substituting any matched nodes
     * with the return value of this method. Generally, classes extending [[ExpressionTransformer]]
     * should override this method and capture the cases that need to be transformed, invoking
     * `super.transform(tree)` for all other nodes.
     *
     * @param expr The [[Expression]] to be transformed
     * @return A modified copy of [[Expression]], transformed according to the rules
     */
    def transform(expr: Expression): Expression = expr match {
      // Monads
      case MonadUnit(xs)                  => MonadUnit(xform(xs))
      case MonadJoin(xs)                  => MonadJoin(xform(xs))
      case Comprehension(h, qs)           => Comprehension(xform(h), qs map xform)
      // Qualifiers
      case Filter(xs)                     => Filter(xform(xs))
      case Generator(lhs, rhs)            => Generator(lhs, xform(rhs))
      // Environment & Host Language Connectors
      case expr: ScalaExpr                => expr
      // Combinators
      case read: c.Read                   => read
      case c.Write(loc, fmt, xs)          => c.Write(loc, fmt, xform(xs))
      case src: c.TempSource              => src
      case c.TempSink(id, xs)             => c.TempSink(id, xform(xs))
      case c.Map(f, xs)                   => c.Map(f, xform(xs))
      case c.FlatMap(f, xs)               => c.FlatMap(f, xform(xs))
      case c.Filter(p, xs)                => c.Filter(p, xform(xs))
      case c.EquiJoin(kx, ky, xs, ys)     => c.EquiJoin(kx, ky, xform(xs), xform(ys))
      case c.Cross(xs, ys)                => c.Cross(xform(xs), xform(ys))
      case c.Group(k, xs)                 => c.Group(k, xform(xs))
      case c.Fold(em, sg, un, xs, or)     => c.Fold(em, sg, un, xform(xs), or)
      case c.FoldGroup(k, em, sg, un, xs) => c.FoldGroup(k, em, sg, un, xform(xs))
      case c.Distinct(xs)                 => c.Distinct(xform(xs))
      case c.Union(xs, ys)                => c.Union(xform(xs), xform(ys))
      case c.Diff(xs, ys)                 => c.Diff(xform(xs), xform(ys))
    }

    protected def xform(e: Expression): Expression =
      transform(e)

    protected def xform(me: MonadExpression): MonadExpression =
      transform(me).as[MonadExpression]

    protected def xform(q: Qualifier): Qualifier =
      transform(q).as[Qualifier]
  }

  // --------------------------------------------------------------------------
  // Helper methods.
  // --------------------------------------------------------------------------

  /**
   * Type-check a [[Tree]] given a set of environment [[ValDef]]s as a closure. Performed only if
   * the [[Tree]] hasn't already been type-checked.
   *
   * @param env A [[List]] of [[ValDef]]s that can be free in the [[Tree]]
   * @param tree The [[Tree]] to be type-checked
   * @return A type-checked version of the [[Tree]]
   */
  def typeCheckWith(env: List[ValDef], tree: Tree) =
    if (tree.hasType) tree else {
      val bindings = for (v <- env.reverse) yield v.name -> q"null.asInstanceOf[${v.tpt}]"
      tree.bind(bindings: _*).typeChecked.as[Block].expr
    }

  /**
   * Pretty-print an IR tree. Currently tries to be smart about trimming some of the generated code,
   * e.g. sugar non-alphanumeric characters and shorten the names of predefined packages like
   * [[scala]], [[scala.Predef]] and [[eu.stratosphere.emma]]
   *
   * @param root The root [[Expression]] of the printed tree
   * @param debug Print in debug mode? (default is `false`)
   * @return A human-readable [[String]] representation of the source code
   */
  def prettyPrint(root: Expression, debug: Boolean = false) = {
    val c = combinator
    val bloat = Seq(
      // non-alphanumeric characters
      "\\$eq"      -> "=",
      "\\$plus"    -> "+",
      "\\$minus"   -> "-",
      "\\$times"   -> "*",
      "\\$div"     -> "/",
      "\\$percent" -> "%",
      "\\$less"    -> "<",
      "\\$greater" -> ">",
      "\\$amp"     -> "&",
      "\\$bar"     -> "|",
      "\\$bang"    -> "!",
      "\\$qmark"   -> "?",
      "\\$up"      -> "^",
      "\\$bslash"  -> "\\",
      "\\$at"      -> "@",
      "\\$hash"    -> "#",
      "\\$tilde"   -> "~",
      "\\$macro"   -> "",
      // apply and unapply
      "\\.apply(\\[[^\\[\\]]\\])?" -> "",
      "\\.unapply"                 -> "",
      // predefined package prefixes
      "(_root_\\.)?eu\\.stratosphere\\.emma\\.api\\." -> "",
      "(_root_\\.)?eu\\.stratosphere\\.emma\\."       -> "",
      "(_root_\\.)?scala\\.collection\\.immutable\\." -> "",
      "(_root_\\.)?scala\\.Predef\\."                 -> "",
      "(_root_\\.)?scala\\.`package`\\."              -> "",
      "(_root_\\.)?scala\\."                          -> "",
       "_root_\\."                                    -> "")

    def pp(x: Any, offset: String = ""): String = x match {

      // Monads
      case MonadJoin(xs) =>
        s"μ ( ${pp(xs, offset + " " * 4)} )"

      case MonadUnit(xs) =>
        s"η ( ${pp(xs, offset + " " * 4)} )"

      case Comprehension(h, qs) => s"""
        |[ ${pp(h, offset + " " * 2)} |
        |  ${offset + pp(qs, offset + " " * 2)} ]
      """.stripMargin.trim // ^${e.tpe.toString}

      // Qualifiers
      case Filter(expr) =>
        s"${pp(expr)}"

      case Generator(lhs, rhs) =>
        s"${lhs.name} ← ${pp(rhs, offset + " " * 3 + " " * lhs.fullName.length)}"

      // Environment & Host Language Connectors
      case ScalaExpr(_, expr) =>
        s"${pp(expr)}"

      // Combinators
      case c.Read(location, _) =>
        s"read (${pp(location)})"

      case c.Write(location, _, xs) => s"""
        |write (${pp(location)}) (
        |       ${offset + pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.TempSource(id) =>
        s"tmpsrc (${pp(id)})"

      case c.TempSink(id, xs) => s"""
        |tmpsnk (${pp(id)}) (
        |       ${offset + pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.Map(f, xs) => s"""
        |map ${pp(f)} (
        |       ${offset + pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.FlatMap(f, xs) => s"""
        |flatMap ${pp(f)} (
        |       ${offset + pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.Filter(p, xs) => s"""
        |filter ${pp(p)} (
        |       ${offset + pp(xs, offset + " " * 8)})
      """.stripMargin.trim

      case c.EquiJoin(kx, ky, xs, ys) => s"""
        |join ${pp(kx)} ${pp(ky)} (
        |       ${offset + pp(xs, offset + " " * 8)} ,
        |       ${offset + pp(ys, offset + " " * 8)} )
      """.stripMargin.trim

      case c.Cross(xs, ys) => s"""
        |cross (
        |       ${offset + pp(xs, offset + " " * 8)} ,
        |       ${offset + pp(ys, offset + " " * 8)} )
      """.stripMargin.trim

      case c.Group(key, xs) => s"""
        |group ${pp(key)} (
        |       ${offset + pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.FoldGroup(key, empty, sng, union, xs) => s"""
        |foldGroup〈${pp(empty)}, ${pp(sng)}, ${pp(union)}〉 ${pp(key)} (
        |       ${offset + pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.Fold(empty, sng, union, xs, _) => s"""
        |fold〈${pp(empty)}, ${pp(sng)}, ${pp(union)}〉(
        |       ${offset + pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.Distinct(xs) => s"""
        |distinct (${pp(xs)})
        |       ${offset + pp(xs, offset)})
      """.stripMargin.trim

      case c.Union(xs, ys) => s"""
        |union (
        |       ${offset + pp(xs, offset + " " * 8)} ,
        |       ${offset + pp(ys, offset + " " * 8)} )
      """.stripMargin.trim

      case c.Diff(xs, ys) => s"""
        |diff (
        |       ${offset + pp(xs, offset + " " * 8)} ,
        |       ${offset + pp(ys, offset + " " * 8)} )
      """.stripMargin.trim

      // Lists
      case xs: List[Any] =>
        xs.map { pp(_, offset) }.mkString(sys.props("line.separator") + offset)

      // Trees
      case tree: Tree => bloat.foldLeft(tree.toString()) {
        case (str, (rex, value)) => str.replaceAll(rex, value)
      }

      // Other types
      case name: TermName => name.toString
      case str:  String   => str
      case _ => "〈unknown expression〉"
    }

    pp(root)
  }
}
