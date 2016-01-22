package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.BlackBoxUtil
import scala.collection.mutable

/** Model and helper functions for the intermediate representation of comprehended terms. */
private[emma] trait ComprehensionModel extends BlackBoxUtil { model =>
  import universe._
  import syntax._

  // --------------------------------------------------------------------------
  // Comprehension API
  // --------------------------------------------------------------------------

  /** A set of API method symbols to be comprehended. */
  protected object api {
    val moduleSymbol     = rootMirror.staticModule("eu.stratosphere.emma.api.package")
    val bagSymbol        = rootMirror.staticClass("eu.stratosphere.emma.api.DataBag")
    val groupSymbol      = rootMirror.staticClass("eu.stratosphere.emma.api.Group")
    val statefulSymbol   = rootMirror.staticClass("eu.stratosphere.emma.api.Stateful.Bag")

    val apply            = bagSymbol.companion.info.decl(TermName("apply"))
    val read             = moduleSymbol.info.decl(TermName("read"))
    val write            = moduleSymbol.info.decl(TermName("write"))
    val stateful         = moduleSymbol.info.decl(TermName("stateful"))
    val fold             = bagSymbol.info.decl(TermName("fold"))
    val map              = bagSymbol.info.decl(TermName("map"))
    val flatMap          = bagSymbol.info.decl(TermName("flatMap"))
    val withFilter       = bagSymbol.info.decl(TermName("withFilter"))
    val groupBy          = bagSymbol.info.decl(TermName("groupBy"))
    val minus            = bagSymbol.info.decl(TermName("minus"))
    val plus             = bagSymbol.info.decl(TermName("plus"))
    val distinct         = bagSymbol.info.decl(TermName("distinct"))
    val fetchToStateless = statefulSymbol.info.decl(TermName("bag"))
    val updateWithZero   = statefulSymbol.info.decl(TermName("updateWithZero"))
    val updateWithOne    = statefulSymbol.info.decl(TermName("updateWithOne"))
    val updateWithMany   = statefulSymbol.info.decl(TermName("updateWithMany"))

    val methods = Set(
      read, write,
      stateful, fetchToStateless, updateWithZero, updateWithOne, updateWithMany,
      fold,
      map, flatMap, withFilter,
      groupBy,
      minus, plus, distinct
    ) ++ apply.alternatives

    val monadic = Set(map, flatMap, withFilter)
    val updateWith = Set(updateWithZero, updateWithOne, updateWithMany)
  }

  // Type constructors
  val DATA_BAG = typeOf[eu.stratosphere.emma.api.DataBag[Nothing]].typeConstructor
  val GROUP = typeOf[eu.stratosphere.emma.api.Group[Nothing, Nothing]].typeConstructor

  // --------------------------------------------------------------------------
  // Comprehension Syntax
  // --------------------------------------------------------------------------

  // TODO: unify with ReflectUtil.syntax.let (with two distinct in methods)

  case class letexpr(bindings: (Symbol, Tree)*) {
    def in(expr: Expression): Expression = expr.substitute { bindings.toMap }
  }

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
    def freeTerms: List[TermSymbol] = {
      val c = combinator
      expr.collect { // Combinators
        case c.Map(f, _)                    => f.closure
        case c.FlatMap(f, _)                => f.closure
        case c.Filter(p, _)                 => p.closure
        case c.EquiJoin(kx, ky, _, _)       => kx.closure | ky.closure
        case c.Group(k, _)                  => k.closure
        case c.Fold(em, sg, un, _, _)       => Set(em, sg, un) flatMap { _.closure }
        case c.FoldGroup(k, em, sg, un, _)  => Set(k, em, sg, un) flatMap { _.closure }
        case c.TempSource(id) if id.hasTerm => Set(id.term)
        case c.StatefulCreate(_, _, _)      => Set.empty[TermSymbol]
        case c.StatefulFetch(id)            => Set(id.term)
        case c.UpdateWithZero(id, f)        => Set(id.term) | f.closure
        case c.UpdateWithOne(id, _, ku, f)  => Set(id.term) | ku.closure | f.closure
        case c.UpdateWithMany(id, _, ku, f) => Set(id.term) | ku.closure | f.closure
      }.flatten.toList.distinct.sortBy { _.name.toString }
    }

    /**
     * Extract the nested Scala trees which can be found under this comprehension.
     * @return The set of [[Tree]] nodes which can be found under this expression
     */
    def trees: Traversable[Tree] = {
      val c = combinator
      expr.collect { // Combinators
        case ScalaExpr(e)                   => Seq(e)
        case c.Map(f, _)                    => Seq(f)
        case c.FlatMap(f, _)                => Seq(f)
        case c.Filter(p, _)                 => Seq(p)
        case c.EquiJoin(kx, ky, _, _)       => Seq(kx, ky)
        case c.Group(k, _)                  => Seq(k)
        case c.Fold(em, sg, un, _, _)       => Seq(em, sg, un)
        case c.FoldGroup(k, em, sg, un, _)  => Seq(k, em, sg, un)
        case c.TempSource(id) if id.hasTerm => Seq(id)
        case c.StatefulCreate(_, _, _)      => Seq()
        case c.StatefulFetch(id)            => Seq(id)
        case c.UpdateWithZero(id, f)        => Seq(id, f)
        case c.UpdateWithOne(id, _, ku, f)  => Seq(id, ku, f)
        case c.UpdateWithMany(id, _, ku, f) => Seq(id, ku, f)
      }.flatten
    }

    /** Bind a dictionary of [[Symbol]]-value pairs in all enclosing trees and assign the result to `expr`. */
    def substitute(dict: Map[Symbol, Tree]): Unit =
      expr = expr.substitute(dict)

    /** Substitute `find` with `replacement` in all enclosing trees and assign the result to `expr`. */
    def replace(find: Tree, replacement: Tree): Unit =
      expr = expr.replace(find, replacement)

    /** Rename `key` as `alias` in all enclosing trees and assign the result to `expr`. */
    def rename(key: Symbol, alias: TermSymbol): Unit =
      expr = expr.rename(key, alias)

    override def toString =
      prettyPrint(expr)
  }

  sealed trait Expression extends Traversable[Expression] {

    /** @return The [[Type]] of this [[Expression]] */
    def tpe: Type

    /** @return The element [[Type]] of this [[Expression]] */
    def elementType: Type = tpe.typeArgs.head

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

    /**
     * Which of the vars defined in the given Comprehension context are used by this monad
     * expression?
     */
    def usedVars(context: Expression): Set[TermSymbol] = {
      // Which variables are defined in the given context?
      val defined = definedVars(context)
      // Which variables are used in the given context?
      val used = collect {
        // Environment & Host Language Connectors
        case ScalaExpr(tree)                                 => Set(tree)
        // Combinators
        case combinator.Map(f, _)                            => Set(f)
        case combinator.FlatMap(f, _)                        => Set(f)
        case combinator.Filter(p, _)                         => Set(p)
        case combinator.EquiJoin(kx, ky, _, _)               => Set(kx, ky)
        case combinator.Group(key, xs)                       => Set(key)
        case combinator.Fold(empty, sng, union, _, _)        => Set(empty, sng, union)
        case combinator.FoldGroup(key, empty, sng, union, _) => Set(key, empty, sng, union)
        case combinator.StatefulCreate(_, _, _)              => Set.empty[Tree]
        case combinator.StatefulFetch(_)                     => Set.empty[Tree]
        case combinator.UpdateWithZero(_, udf)               => Set(udf)
        case combinator.UpdateWithOne(_, _, key, udf)        => Set(key, udf)
        case combinator.UpdateWithMany(_, _, key, udf)       => Set(key, udf)
      }.flatten.flatMap { _.closure }.toSet
      // Result is the intersection of both
      defined & used
    }

    /**
     * Which generator-defined vars in the given Comprehension context are visible in this monad
     * expression?
     */
    def definedVars(context: Expression): Set[TermSymbol] = {
      val cet = new CollectEnvironmentTraverser(this)
      cet.traverse(context)
      cet.env
    }

    /** Bind a dictionary of [[Symbol]]-value pairs in all enclosing trees. */
    def substitute(dict: Map[Symbol, Tree]): Expression =
      new ExpressionTransformer {
        override def xform(tree: Tree) = model.substitute(tree)(dict)
      }.transform(this)

    /** Substitute `find` with `replacement` in all enclosing trees. */
    def replace(find: Tree, replacement: Tree): Expression =
      new ExpressionTransformer {
        override def xform(tree: Tree) = model.replace(tree)(find, replacement)
      }.transform(this)

    /** Rename `key` as `alias` in all enclosing trees. */
    def rename(key: Symbol, alias: TermSymbol): Expression =
      new ExpressionTransformer {
        override def xform(tree: Tree) = model.rename(tree, key, alias)
      }.transform(this)
  }

  // Monads

  sealed trait MonadExpression extends Expression

  case class MonadJoin(expr: MonadExpression) extends MonadExpression {
    def tpe = DATA_BAG(expr.tpe)
    def descend[U](f: Expression => U) = expr foreach f
  }

  case class MonadUnit(expr: MonadExpression) extends MonadExpression {
    def tpe = DATA_BAG(expr.tpe)
    def descend[U](f: Expression => U) = expr foreach f
  }

  case class Comprehension(hd: Expression, qualifiers: List[Qualifier])
      extends MonadExpression {

    def tpe = DATA_BAG(hd.tpe)

    def descend[U](f: Expression => U) = {
      for (q <- qualifiers) q foreach f
      hd foreach f
    }
  }

  // Qualifiers

  sealed trait Qualifier extends Expression

  case class Guard(expr: Expression) extends Qualifier {
    def tpe = typeOf[Boolean]
    def descend[U](f: Expression => U) = expr foreach f
  }

  case class Generator(var lhs: TermSymbol, var rhs: Expression) extends Qualifier {
    def tpe = rhs.elementType
    def descend[U](f: Expression => U) = rhs foreach f
  }

  // Environment & Host Language Connectors

  case class ScalaExpr(tree: Tree) extends Expression {
    def tpe = tree.preciseType
    def descend[U](f: Expression => U) = ()
  }

  // Combinators

  object combinator {

    sealed trait Combinator extends Expression

    case class Read(location: Tree, format: Tree) extends Combinator {
      def tpe = DATA_BAG(format.preciseType.typeArgs.head)
      def descend[U](f: Expression => U) = ()
    }

    case class Write(location: Tree, format: Tree, in: Expression) extends Combinator {
      def tpe = typeOf[Unit]
      def descend[U](f: Expression => U) = in foreach f
    }

    case class TempSource(id: Ident) extends Combinator {
      val tpe = id.preciseType
      def descend[U](f: Expression => U) = ()
    }

    case class TempSink(id: TermName, xs: Expression) extends Combinator {
      val tpe = xs.tpe
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class Map(f: Tree, xs: Expression) extends Combinator {
      def tpe = DATA_BAG(f.preciseType.typeArgs.last)
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class FlatMap(f: Tree, xs: Expression) extends Combinator {
      // Since the return type of f is DataBag[DataBag[T]], don't wrap!
      def tpe = f.preciseType.typeArgs.last
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class Filter(p: Tree, xs: Expression) extends Combinator {
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

    case class Group(key: Tree, xs: Expression) extends Combinator {

      def tpe = {
        val K = key.preciseType.typeArgs(1)
        val V = DATA_BAG(xs.elementType)
        DATA_BAG(GROUP(K, V))
      }

      def descend[U](f: Expression => U) =
        xs foreach f
    }

    case class Fold(empty: Tree, sng: Tree, union: Tree, xs: Expression, origin: Tree) extends Combinator {
      def tpe = sng.preciseType.typeArgs(1) // Function[A, B]#B
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class FoldGroup(key: Tree, empty: Tree, sng: Tree, union: Tree, xs: Expression) extends Combinator {

      def tpe = {
        val K = key.preciseType.typeArgs(1)
        val V = sng.preciseType.typeArgs(1)
        DATA_BAG(GROUP(K, V))
      }

      def descend[U](f: Expression => U) =
        xs foreach f
    }

    case class Distinct(xs: Expression) extends Combinator {
      def tpe = xs.tpe
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class Union(xs: Expression, ys: Expression) extends Combinator {

      def tpe = ys.tpe

      def descend[U](f: Expression => U) = {
        xs foreach f
        ys foreach f
      }
    }

    case class Diff(xs: Expression, ys: Expression) extends Combinator {

      def tpe = ys.tpe

      def descend[U](f: Expression => U) = {
        xs foreach f
        ys foreach f
      }
    }

    case class StatefulCreate(xs: Expression, stateType: Type, keyType: Type)
        extends Combinator {
      def tpe = typeOf[Unit]
      def descend[U](f: Expression => U) = xs foreach f
    }

    case class StatefulFetch(stateful: Ident) extends Combinator {
      def tpe = DATA_BAG(stateful.preciseType.typeArgs.head)
      def descend[U](f: Expression => U) = ()
    }

    case class UpdateWithZero(stateful: Ident, udf: Tree) extends Combinator {
      def tpe = udf.preciseType.typeArgs.last
      def descend[U](f: Expression => U) = ()
    }

    case class UpdateWithOne(stateful: Ident, updates: Expression, key: Tree, udf: Tree)
        extends Combinator {
      def tpe = udf.preciseType.typeArgs.last
      def descend[U](f: Expression => U) = updates foreach f
    }

    case class UpdateWithMany(stateful: Ident, updates: Expression, key: Tree, udf: Tree)
        extends Combinator {
      def tpe = udf.preciseType.typeArgs.last
      def descend[U](f: Expression => U) = updates foreach f
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
      defIndex.get(definition)

    def getByTerm(term: Tree): Option[ComprehendedTerm] =
      termIndex.get(term)

    def remove(ct: ComprehendedTerm): Unit = {
      for (df <- ct.definition) defIndex -= df // remove from defIndex
      termIndex -= ct.term // remove from termIndex
    }
  }

  case class ComprehendedTerm(id: TermName, term: Tree,
      comprehension: ExpressionRoot, definition: Option[Tree]) {

    /**
      * Get the [[Position]] of this [[ComprehendedTerm]] in the original source code.
      *
      * NOTE:
      * - [[Position]]s use 1-based indexing.
      * - To get a range [[Position]], the code has to be compiled with `-Yrangepos`.
      * - In some corner cases (e.g. nested macro expansion), always returns a point.
      *
      * @return the [[Position]] in the original code
      */
    def pos: Position = term.pos

    /**
      * Get the original source code that resulted in this [[ComprehendedTerm]].
      *
      * NOTE:
      * - To get the full source code, it has to be compiled with `-Yrangepos`.
      * - Without `-Yrangepos` and in some corner cases, always returns one line.
      * - Inlined code is currently not included.
      *
      * @return the original source code as a [[String]]
      */
    def src: String = {
      val source = pos.source
      val code = if (pos.isRange) {
        val start = pos.start - 1
        val length = pos.end - start
        new String(source.content, start, length)
      } else source.lineToString(pos.line - 1)
      code.trim
    }
  }

  // --------------------------------------------------------------------------
  // Transformation & Traversal
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
      case MonadUnit(xs)                       => MonadUnit(xform(xs))
      case MonadJoin(xs)                       => MonadJoin(xform(xs))
      case Comprehension(h, qs)                => Comprehension(xform(h), qs map xform)
      // Qualifiers
      case Guard(xs)                          => Guard(xform(xs))
      case Generator(lhs, rhs)                 => Generator(lhs, xform(rhs))
      // Environment & Host Language Connectors
      case ScalaExpr(tree)                     => ScalaExpr(xform(tree))
      // Combinators
      case read: c.Read                        => read
      case c.Write(loc, fmt, xs)               => c.Write(xform(loc), xform(fmt), xform(xs))
      case src: c.TempSource                   => src
      case c.TempSink(id, xs)                  => c.TempSink(id, xform(xs))
      case c.Map(f, xs)                        => c.Map(xform(f), xform(xs))
      case c.FlatMap(f, xs)                    => c.FlatMap(xform(f), xform(xs))
      case c.Filter(p, xs)                     => c.Filter(xform(p), xform(xs))
      case c.EquiJoin(kx, ky, xs, ys)          => c.EquiJoin(xform(kx), xform(ky), xform(xs), xform(ys))
      case c.Cross(xs, ys)                     => c.Cross(xform(xs), xform(ys))
      case c.Group(k, xs)                      => c.Group(xform(k), xform(xs))
      case c.Fold(em, sg, un, xs, or)          => c.Fold(xform(em), xform(sg), xform(un), xform(xs), xform(or))
      case c.FoldGroup(k, em, sg, un, xs)      => c.FoldGroup(xform(k), xform(em), xform(sg), xform(un), xform(xs))
      case c.Distinct(xs)                      => c.Distinct(xform(xs))
      case c.Union(xs, ys)                     => c.Union(xform(xs), xform(ys))
      case c.Diff(xs, ys)                      => c.Diff(xform(xs), xform(ys))
      case c.StatefulCreate(xs, stTpe, keyTpe) => c.StatefulCreate(xform(xs), stTpe, keyTpe)
      case c.StatefulFetch(stateful)           => c.StatefulFetch(stateful)
      case c.UpdateWithMany(s, us, kSel, f)    => c.UpdateWithMany(s, xform(us), kSel, f)
      case c.UpdateWithOne(s, us, kSel, f)     => c.UpdateWithOne(s, xform(us), kSel, f)
      case c.UpdateWithZero(s, f)              => c.UpdateWithZero(s, f)
    }

    protected def xform(e: Expression): Expression =
      transform(e)

    protected def xform(me: MonadExpression): MonadExpression =
      transform(me).as[MonadExpression]

    protected def xform(q: Qualifier): Qualifier =
      transform(q).as[Qualifier]

    protected def xform(t: Tree): Tree = t
  }

  trait ExpressionTraverser {

    def traverse(e: Expression): Unit = e match {
      // Monads
      case MonadUnit(expr)                        => traverse(expr)
      case MonadJoin(expr)                        => traverse(expr)
      case Comprehension(head, qualifiers)        => qualifiers foreach traverse; traverse(head)
      // Qualifiers
      case Guard(expr) => traverse(expr)
      case Generator(_, rhs)                      => traverse(rhs)
      // Environment & Host Language Connectors
      case ScalaExpr(_)                           =>
      // Combinators
      case combinator.Read(_, _)                  =>
      case combinator.Write(_, _, xs)             => traverse(xs)
      case combinator.TempSource(_)               =>
      case combinator.TempSink(_, xs)             => traverse(xs)
      case combinator.Map(_, xs)                  => traverse(xs)
      case combinator.FlatMap(_, xs)              => traverse(xs)
      case combinator.Filter(_, xs)               => traverse(xs)
      case combinator.EquiJoin(_, _, xs, ys)      => traverse(xs); traverse(ys)
      case combinator.Cross(xs, ys)               => traverse(xs); traverse(ys)
      case combinator.Group(_, xs)                => traverse(xs)
      case combinator.Fold(_, _, _, xs, _)        => traverse(xs)
      case combinator.FoldGroup(_, _, _, _, xs)   => traverse(xs)
      case combinator.Distinct(xs)                => traverse(xs)
      case combinator.Union(xs, ys)               => traverse(xs); traverse(ys)
      case combinator.Diff(xs, ys)                => traverse(xs); traverse(ys)
      case combinator.StatefulCreate(xs, _, _)    => traverse(xs)
      case combinator.StatefulFetch(_)            =>
      case combinator.UpdateWithZero(_, _)        =>
      case combinator.UpdateWithOne(_, us, _, _)  => traverse(us)
      case combinator.UpdateWithMany(_, us, _, _) => traverse(us)
    }
  }

  private class CollectEnvironmentTraverser(needle: Expression) extends ExpressionTraverser {
    private val S = mutable.Stack[mutable.Set[TermSymbol]]()
    private var T = false

    def env = S.toSet.flatten

    override def traverse(t: Expression) = if (!T) {
      if (t == needle) T = true
      else t match {
        case Comprehension(head, qualifiers) =>
          S.push(mutable.Set.empty[TermSymbol])
          qualifiers foreach traverse
          traverse(head)
          if (!T) S.pop()

        case Generator(lhs, rhs) =>
          traverse(rhs)
          S.top += lhs

        case _ => super.traverse(t)
      }
    }
  }

  // --------------------------------------------------------------------------
  // Helper methods.
  // --------------------------------------------------------------------------

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
      case Guard(expr) =>
        s"${pp(expr)}"

      case Generator(lhs, rhs) =>
        s"${lhs.name} ← ${pp(rhs, offset + " " * 3 + " " * lhs.fullName.length)}"

      // Environment & Host Language Connectors
      case ScalaExpr(expr) =>
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
        |distinct (
        |       ${offset + pp(xs, offset + " " * 8)})
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

      case c.StatefulCreate(xs, stateTpe, keyTpe) => s"""
        |statefulCreate[$stateTpe, $keyTpe] (
        |       ${pp(xs, offset + " " * 8)} )
      """.stripMargin.trim

      case c.StatefulFetch(stateful) => s"""
        |statefulFetch (${pp(stateful)})
      """.stripMargin.trim

      case c.UpdateWithZero(stateful, udf) => s"""
        |UpdateWithZero (
        |       ${pp(stateful, offset + " " * 8)},
        |       ${pp(udf,      offset + " " * 8)} )
      """.stripMargin.trim

      case c.UpdateWithOne(stateful, us, uKeySel, udf) => s"""
        |UpdateWithOne (
        |       ${pp(stateful, offset + " " * 8)},
        |       ${pp(us,       offset + " " * 8)},
        |       ${pp(uKeySel,  offset + " " * 8)},
        |       ${pp(udf,      offset + " " * 8)} )
      """.stripMargin.trim

      case c.UpdateWithMany(stateful, us, uKeySel, udf) => s"""
        |UpdateWithMany (
        |       ${pp(stateful, offset + " " * 8)},
        |       ${pp(us,       offset + " " * 8)},
        |       ${pp(uKeySel,  offset + " " * 8)},
        |       ${pp(udf,      offset + " " * 8)} )
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

  def unComprehend(expr: Expression): Tree = (expr match {

    // -----------------------------------------------------
    // Monad Ops
    // -----------------------------------------------------

    // for { x <- xs; y <- ys; if p; if q } yield f(x, y)
    case Comprehension(head, Generator(lhs, rhs) :: FilterPrefix(fs, qs)) =>
      // apply all filters to the uncomprehended 'rhs' first
      val xs = fs.foldLeft(unComprehend(rhs)) { (result, filter) =>
        q"$result.withFilter(${lambda(lhs) { unComprehend(filter.expr) }})"
      }

      // append a map or a flatMap to the result depending on the size of the residual qualifier
      // sequence 'qs'
      qs match {
        case Nil => q"$xs.map(${lambda(lhs) { unComprehend(head) }})"
        case _ => q"$xs.flatMap(${lambda(lhs) { unComprehend(Comprehension(head, qs)) }})"
      }

    // xs map f
    case combinator.Map(f, xs) =>
      q"${unComprehend(xs)}.map($f)"

    // xs withFilter f
    case combinator.Filter(p, xs) =>
      q"${unComprehend(xs)}.withFilter($p)"

    // xs.flatten
    case MonadJoin(xs) =>
      q"${unComprehend(xs)}.flatMap(_root_.scala.Predef.identity)"

    // xs flatMap f
    case combinator.FlatMap(f, xs) =>
      q"${unComprehend(xs)}.flatMap($f)"

    // -----------------------------------------------------
    // Joins
    // -----------------------------------------------------

    // xs join ys where kx equalTo ky
    case combinator.EquiJoin(kx, ky, xs, ys) =>
      val $(x, y) = $("unComp$x", "unComp$y")
      q"""for {
        $x <- ${unComprehend(xs)}
        $y <- ${unComprehend(ys)}
        if $kx($x) == $ky($y)
      } yield ($x, $y)"""

    // xs cross ys
    case combinator.Cross(xs, ys) =>
      val $(x, y) = $("unComp$x", "unComp$y")
      q"""for {
        $x <- ${unComprehend(xs)}
        $y <- ${unComprehend(ys)}
      } yield ($x, $y)"""

    // -----------------------------------------------------
    // Grouping and Set operations
    // -----------------------------------------------------

    // xs groupBy key
    case combinator.Group(key, xs) =>
      q"${unComprehend(xs)}.groupBy($key)"

    // xs minus ys
    case combinator.Diff(xs, ys) =>
      q"${unComprehend(xs)}.minus(${unComprehend(ys)})"

    // xs plus ys
    case combinator.Union(xs, ys) =>
      q"${unComprehend(xs)}.plus(${unComprehend(ys)})"

    // xs.distinct()
    case combinator.Distinct(xs) =>
      q"${unComprehend(xs)}.distinct()"

    // -----------------------------------------------------
    // Aggregates
    // -----------------------------------------------------

    // xs.fold(empty)(sng, union)
    case combinator.Fold(empty, sng, union, xs, _) =>
      q"${unComprehend(xs)}.fold($empty)($sng, $union)"

    // xs groupBy key map { g => Group(g.key, g.values.fold(empty)(sng, union)) }
    case combinator.FoldGroup(key, empty, sng, union, xs) =>
      val g = $"unComp$$g"
      q"""${unComprehend(xs)}.groupBy($key).map({ case $g =>
        _root_.eu.stratosphere.emma.api.Group($g.key, $g.values.fold($empty)($sng, $union))
      })"""

    // -----------------------------------------------------
    // Stateful data bags
    // -----------------------------------------------------

    // stateful[S, K](xs)
    case combinator.StatefulCreate(xs, stType, keyType) =>
      q"_root_.eu.stratosphere.emma.api.stateful[$stType, $keyType](${unComprehend(xs)})"

    // stateful.bag()
    case combinator.StatefulFetch(stateful) =>
      q"$stateful.bag()"

    // stateful updateWithZero udf
    case combinator.UpdateWithZero(stateful, udf) =>
      q"$stateful.updateWithZero($udf)"

    // stateful.updateWithOne(updates)(key, udf)
    case combinator.UpdateWithOne(stateful, updates, key, udf) =>
      q"$stateful.updateWithOne(${unComprehend(updates)})($key, $udf)"

    // stateful.updateWithMany(updates)(key, udf)
    case combinator.UpdateWithMany(stateful, updates, key, udf) =>
      q"$stateful.updateWithMany(${unComprehend(updates)})($key, $udf)"

    // ----------------------------------------------------------------------
    // Environment & Host Language Connectors
    // ----------------------------------------------------------------------

    // write[T](loc, fmt)(xs)
    case combinator.Write(loc, fmt, xs) =>
      q"_root_.eu.stratosphere.emma.api.write($loc, $fmt)(${unComprehend(xs)})"

    // read[T](loc, fmt)
    case combinator.Read(loc, fmt) =>
      q"_root_.eu.stratosphere.emma.api.read($loc, $fmt)"

    // Temp sink identifier
    case combinator.TempSink(id, xs) =>
      q"val $id = ${unComprehend(xs)}"

    // Temp result identifier
    case combinator.TempSource(id) => id

    // Scala expression
    case ScalaExpr(tree) => tree

    case _ =>
      val combinator = expr.getClass.getSimpleName
      throw new UnsupportedOperationException(s"Unsupported combinator: $combinator")
  }).typeChecked

  /** Helper pattern matcher. Matches a prefix of Filters in a Qualifier sequence. */
  object FilterPrefix {
    def unapply(qs: List[Qualifier]): Option[(List[Guard], List[Qualifier])] = {
      val (filters, rest) = qs span { case _: Guard => true; case _ => false }
      Some(filters.as[List[Guard]], rest)
    }
  }
}
