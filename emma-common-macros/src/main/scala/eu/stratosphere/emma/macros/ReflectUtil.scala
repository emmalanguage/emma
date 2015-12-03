package eu.stratosphere.emma.macros

import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.reflect.api.Universe

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros and runtime reflection APIs, e.g. non-idempotent type checking, lack of hygiene,
 * capture-avoiding substitution, fully-qualified names, fresh name generation, identifying
 * closures, etc.
 *
 * This trait has to be instantiated with a [[Universe]] type and works for both runtime and
 * compile time reflection.
 */
trait ReflectUtil {
  val universe: Universe

  import universe._
  import internal.reificationSupport._
  import syntax._

  // Predefined types

  def UNIT   = definitions.UnitTpe
  def BOOL   = definitions.BooleanTpe
  def CHAR   = definitions.CharTpe
  def BYTE   = definitions.ByteTpe
  def SHORT  = definitions.ShortTpe
  def INT    = definitions.IntTpe
  def LONG   = definitions.LongTpe
  def FLOAT  = definitions.FloatTpe
  def DOUBLE = definitions.DoubleTpe

  lazy val STRING  = typeOf[String]
  lazy val BIG_INT = typeOf[BigInt]
  lazy val BIG_DEC = typeOf[BigDecimal]

  lazy val VOID      = typeOf[java.lang.Void]
  lazy val J_BOOL    = typeOf[java.lang.Boolean]
  lazy val J_CHAR    = typeOf[java.lang.Character]
  lazy val J_BYTE    = typeOf[java.lang.Byte]
  lazy val J_SHORT   = typeOf[java.lang.Short]
  lazy val J_INT     = typeOf[java.lang.Integer]
  lazy val J_LONG    = typeOf[java.lang.Long]
  lazy val J_FLOAT   = typeOf[java.lang.Float]
  lazy val J_DOUBLE  = typeOf[java.lang.Double]
  lazy val J_BIG_INT = typeOf[java.math.BigInteger]
  lazy val J_BIG_DEC = typeOf[java.math.BigDecimal]

  lazy val PAIR = TUPLE(2)
  lazy val ARRAY = typeOf[Array[Nothing]].typeConstructor

  lazy val TUPLE = Map(1 to 22 map { n =>
    n -> rootMirror.staticClass(s"scala.Tuple$n").toTypeConstructor
  }: _*)

  lazy val FUN = Map(0 to 22 map { n =>
    n -> rootMirror.staticClass(s"scala.Function$n").toTypeConstructor
  }: _*)

  /**
   * Parse a [[String]] of source code.
   *
   * @param string The source code to parse
   * @return A [[Tree]] representation of the source code
   */
  def parse(string: String): Tree

  /**
   * Type-check a source code snippet.
   * @param tree The [[Tree]] to type-check
   * @return A [[Tree]] annotated with [[Type]]s and [[Symbol]]s
   */
  def typeCheck(tree: Tree): Tree

  /** Create a new [[TermSymbol]]. */
  def termSym(owner: Symbol, name: TermName, tpe: Type,
    flags: FlagSet = Flag.SYNTHETIC,
    pos: Position = NoPosition): TermSymbol

  /** Create a new [[TypeSymbol]]. */
  def typeSym(owner: Symbol, name: TypeName,
    flags: FlagSet = Flag.SYNTHETIC,
    pos: Position = NoPosition): TypeSymbol

  /** Remove all [[Type]] annotations from this [[Tree]]. */
  // FIXME: Replace with c.untypecheck once https://issues.scala-lang.org/browse/SI-5464 resolved
  val unTypeCheck = { showCode(_: Tree, printRootPkg = true) } andThen parse

  /** Alias for `typeCheck compose unTypeCheck`. */
  val reTypeCheck = unTypeCheck andThen typeCheck

  /** Alias for `typeCheck compose parse`. */
  val parseCheck = parse _ andThen typeCheck

  /**
   * Recursively apply a depth-first transformation to a [[Tree]].
   * @param tree The [[Tree]] to transform
   * @param pf A [[PartialFunction]] to transform some of the [[Tree]] nodes
   * @return A new [[Tree]] with some of the nodes transformed
   */
  def transform(tree: Tree)(pf: Tree ~> Tree): Tree = new Transformer {
    override def transform(tree: Tree): Tree =
      if (pf.isDefinedAt(tree)) pf(tree)
      else super.transform(tree)
  }.transform(tree)

  /**
   * Recursively apply a depth-first traversal to a [[Tree]].
   * @param tree The [[Tree]] to traverse
   * @param pf A [[PartialFunction]] to traverse some of the [[Tree]] nodes
   */
  def traverse(tree: Tree)(pf: Tree ~> Unit): Unit = new Traverser {
    override def traverse(tree: Tree): Unit =
      if (pf.isDefinedAt(tree)) pf(tree)
      else super.traverse(tree)
  }.traverse(tree)

  /**
   * Recursively remove all layers of type ascriptions from a [[Tree]].
   * E.g. `q"((42: Int): Int)".unAscribed = q"42"`
   * @param tree The [[Tree]] to remove ascriptions from
   * @return An equivalent [[Tree]] without type ascriptions
   */
  @tailrec final def unAscribed(tree: Tree): Tree = tree match {
    case q"${t: Tree}: $_" => unAscribed(t)
    case _ => tree
  }

  /**
   * Bind a sequence of name-value pairs in a [[Tree]].
   * @param in The [[Tree]] to bind in
   * @param bindings A sequence of name-value pairs to bind
   * @return This [[Tree]] with all names in `kvs` bound to their respective values
   */
  def bind(in: Tree)(bindings: (TermName, Tree)*): Tree =
    if (bindings.isEmpty) in else
      q"{ ..${for ((k, v) <- bindings) yield q"val $k = $v"}; $in }"

  /**
   * Bind a dictionary of [[Symbol]]-value pairs in a [[Tree]].
   * @param in The tree to substitute in
   * @param dict A [[Map]] of [[Symbol]]-value pairs to bind
   * @return This [[Tree]] with all [[Symbol]]s in `dict` bound to their respective values
   */
  def substitute(in: Tree)(dict: Map[Symbol, Tree]): Tree =
    if (dict.isEmpty) in else {
      val closure = dict.values
        .flatMap { _.closure }
        .filterNot(dict.keySet)
        .map { _.name }.toSet

      val capture = in.definitions filter { term => closure(term.name) }
      refresh(in)(capture.toSeq: _*) ->> (transform(_) {
        case id: Ident if dict contains id.symbol => dict(id.symbol)
      })
    }

  /**
   * Replace occurrences of the `find` [[Tree]] with the `replacement` [[Tree]].
   * @param in The [[Tree]] to substitute in
   * @param find The [[Tree]] to look for
   * @param replacement The [[Tree]] that should replace `find`
   * @return A substituted version of the enclosing [[Tree]]
   */
  def replace(in: Tree)(find: Tree, replacement: Tree): Tree =
    transform(in) { case `find` => replacement }

  /**
   * Replace a sequence of [[Symbol]]s in a [[Tree]] with fresh ones.
   * @param in The [[Tree]] to refresh
   * @param symbols The sequence of [[Symbol]]s to rename
   * @return This [[Tree]] with the specified [[Symbol]]s replaced
   */
  def refresh(in: Tree)(symbols: Symbol*): Tree =
    rename(in, { for (sym <- symbols)
      yield sym -> termSym(sym.owner, $"${sym.name}", sym.info, pos = sym.pos)
    }: _*)

  /**
   * Replace a dictionary of [[Symbol]]s with references to their aliases.
   * @param in The [[Tree]] to rename in
   * @param dict A [[Map]] of aliases to replace
   * @return This [[Tree]] with the specified [[Symbol]]s replaced 
   */
  def rename(in: Tree, dict: Map[Symbol, TermSymbol]): Tree =
    if (dict.isEmpty) in else transform(in) {
      case vd: ValDef if dict contains vd.symbol => val_(dict(vd.symbol)) := vd.rhs
      case id: Ident  if dict contains id.symbol => &(dict(id.symbol))
      case bd: Bind   if dict contains bd.symbol => dict(bd.symbol) @@ bd.body
    }

  /**
   * Replace a sequence of [[Symbol]]s with references to their aliases.
   * @param in The [[Tree]] to rename in
   * @param aliases A sequence of aliases to replace
   * @return This [[Tree]] with the specified [[Symbol]]s replaced
   */
  def rename(in: Tree, aliases: (Symbol, TermSymbol)*): Tree =
    rename(in, aliases.toMap)

  /**
   * Replace a [[Symbol]]s with a reference to an alias.
   * @param in The [[Tree]] to rename in
   * @param key The [[Symbol]] to rename
   * @param alias An alternative [[Symbol]] to use
   * @return This [[Tree]] with the specified [[Symbol]] replaced
   */
  def rename(in: Tree, key: Symbol, alias: TermSymbol): Tree =
    rename(in, key -> alias)

  /**
   * Inline a value definitions in a [[Tree]]. It's assumed that it's part of the [[Tree]].
   * @param in The [[Tree]] to inline in
   * @param valDef the [[ValDef]] to inline
   * @return this [[Tree]] with the specified value definitions inlined
   */
  def inline(in: Tree, valDef: ValDef): Tree = transform(in) {
    case vd: ValDef if vd.symbol == valDef.symbol => q"()"[Unit]
    case id: Ident  if id.symbol == valDef.symbol => valDef.rhs
  }

  /** [[Tree]], [[Name]] and [[Symbol]] constructors. */
  object mk {

    /** Generate a new [[TermName]]. */
    def freshName(prefix: String): TermName =
      if (prefix.nonEmpty && prefix.last == '$') freshTermName(prefix)
      else freshTermName(s"$prefix$$")

    /** Generate a new [[TypeName]]. */
    def freshType(prefix: String): TypeName =
      if (prefix.nonEmpty && prefix.last == '$') freshTypeName(prefix)
      else freshTypeName(s"$prefix$$")

    def ref(term: TermSymbol): Ident =
      Ident(term.name).withSym(term).as[Ident]

    def bind(term: TermSymbol,
        pattern: Tree = Ident(termNames.WILDCARD)): Bind =
      Bind(term.name, pattern).withSym(term).as[Bind]

    def freeTerm(name: String, tpe: Type,
        flags: FlagSet = Flag.SYNTHETIC,
        origin: String = null): TermSymbol =
      newFreeTerm(name, null, flags, origin).withType(tpe).asTerm

    def freeType(name: String,
        flags: FlagSet = Flag.SYNTHETIC,
        origin: String = null): TypeSymbol =
      newFreeType(name, flags, origin)

    def valDef(term: TermSymbol): ValDef =
      valDef(term, EmptyTree)

    def valDef(term: TermSymbol, rhs: Tree): ValDef =
      internal.valDef(term, rhs)

    def valDef(name: TermName, tpe: Type,
        owner: Symbol = NoSymbol,
        flags: FlagSet = Flag.SYNTHETIC,
        pos: Position = NoPosition,
        rhs: Tree = EmptyTree): ValDef =
      valDef(termSym(owner, name, tpe.precise, flags, pos), rhs)

    def anonFun(args: List[TermSymbol], body: Tree,
        owner: Symbol = NoSymbol,
        flags: FlagSet = Flag.SYNTHETIC,
        pos: Position = NoPosition): Function = {
      val argTypes = args map { _.preciseType }
      val funType = FUN(args.size)(argTypes :+ body.preciseType: _*)
      val funSym = termSym(owner, TermName("anonfun"), funType, flags, pos)
      val params = for { (arg, tpe) <- args zip argTypes }
        yield valDef(arg.name, tpe, funSym, Flag.SYNTHETIC | Flag.PARAM, pos)

      val tree = rename(body, args zip params.map { _.term }: _*)
      Function(params, tree).withSym(funSym).as[Function]
    }

    def select(symbol: Symbol): Tree =
      if (Set(null, NoSymbol, rootMirror.RootClass) contains symbol.owner)
        &(rootMirror.staticPackage(symbol.fullName))
      else q"${select(symbol.owner)}.${symbol.name.toTermName}"(symbol)

    def typeSelect(symbol: Symbol): Tree =
      if (Set(null, NoSymbol, rootMirror.RootClass) contains symbol.owner)
        &(rootMirror.staticPackage(symbol.fullName))
      else tq"${select(symbol.owner)}.${symbol.name.toTypeName}"(symbol)
  }
  
  object syntax {
    
    /** Alias of [[PartialFunction]]. */
    type ~>[A, B] = PartialFunction[A, B]
    def &(term: TermSymbol): Ident = mk.ref(term)
    def $(prefixes: String*): Seq[TermName] = prefixes map mk.freshName
    def val_(term: TermSymbol): ValDef = mk.valDef(term)
    def val_(name: TermName, tpe: Type): ValDef = mk.valDef(name, tpe)
    def val_(name: String, tpe: Type): ValDef = val_(TermName(name), tpe)
    def λ(args: TermSymbol*)(body: Tree): Function = lambda(args: _*) { body }
    def lambda(args: TermSymbol*)(body: Tree): Function = mk.anonFun(args.toList, body)

    case class let(bindings: (Symbol, Tree)*) {
      def in(tree: Tree): Tree = substitute(tree) { bindings.toMap }
    }

    object $ {
      def unapplySeq(terms: Seq[TermName]): Option[Seq[TermName]] = Some(terms)
    }

    implicit class FreshNameGen(self: StringContext) {
      def $(args: Any*): TermName = mk.freshName(self.s(args: _*))
    }

    /** Syntax sugar for class casting. */
    implicit class As[A](self: A) {
      /** Alias for `this.asInstanceOf[B]`. */
      def as[B]: B = self.asInstanceOf[B]
    }

    /** Syntax sugar for function call chain emulation. */
    implicit class Chain[A](self: A) {

      /**
       * Emulation of a function call chain starting with `this`, similar to Clojure's thread macros
       * (-> and/or ->>). E.g. `x ->> f ->> g == g(f(x)) == (g compose f)(x) == (f andThen g)(x)`.
       * @param f Next function to thread in the call chain
       * @tparam B Return type of the next function
       */
      def ->>[B](f: A => B): B = f(self)

      /** Alias for `this ->> f`. */
      def chain[B](f: A => B): B = f(self)
    }

    implicit class ValDefOps(self: ValDef) {
      def :=(rhs: Tree): ValDef = mk.valDef(self.term, rhs)
    }

    implicit class TermSymbolOps(self: TermSymbol) {
      def @@(pattern: Tree): Bind = mk.bind(self, pattern)
    }

    /** Syntax sugar for [[Type]]s. */
    implicit class TypeOps(self: Type) {

      def ::(tree: Tree): Tree = tree withType self
      def ::(symbol: Symbol): Symbol = symbol withType self

      /** @return The de-aliased and widened version of this [[Type]] */
      def precise: Type = self.finalResultType.widen

      /**
       * Apply this [[Type]] as a [[Type]] constructor.
       * @param args The [[Type]] arguments
       * @return The specialized version of this [[Type]]
       */
      def apply(args: Type*): Type =
        appliedType(self, args: _*)
    }

    /** Syntax sugar for [[Symbol]]s. */
    implicit class SymbolOps(self: Symbol) {

      def apply(tpe: Type): Symbol = withType(tpe)
      def apply[T: TypeTag]: Symbol = withType[T]

      /** Check if this [[Symbol]] has an associated [[Type]]. */
      def hasType: Boolean = self.info != null && self.info != NoType

      /** Set the `info` of this [[Symbol]]. */
      def withType(tpe: Type): Symbol = setInfo(self, tpe.precise)

      /** Set the `info` of this [[Symbol]]. */
      def withType[T: TypeTag]: Symbol = withType(typeOf[T])

      /** @return The most precise [[Type]] of this [[Symbol]] */
      def preciseType: Type = self.info.precise
    }

    /** Syntactic sugar for [[Tree]]s. */
    implicit class TreeOps(self: Tree) {

      def apply(tpe: Type): Tree = withType(tpe)
      def apply[T: TypeTag]: Tree = withType[T]
      def apply(symbol: Symbol): Tree = withSym(symbol)
      def apply(position: Position): Tree = at(position)

      def ^(symbol: Symbol): Tree = withSym(symbol)
      def @@(position: Position): Tree = at(position)

      /** @return `true` if this [[Tree]] is annotated with a [[Type]], `false` otherwise */
      def hasType: Boolean = self.tpe != null && self.tpe != NoType

      /** @return `true` if this [[Tree]] is annotated with a [[Symbol]], `false` otherwise */
      def hasSymbol: Boolean = self.symbol != null && self.symbol != NoSymbol

      /** @return An un-type-checked version of this [[Tree]] */
      def unTypeChecked: Tree = unTypeCheck(self)

      /** @return `unTypeChecked.typeChecked` */
      def reTypeChecked: Tree = reTypeCheck(self)

      /** @return The most precise [[Type]] of this [[Tree]] */
      def preciseType: Type =
        if (hasType) self.tpe.precise
        else if (hasSymbol && self.symbol.hasType) self.symbol.preciseType
        else typeChecked.tpe.precise

      /** @return The [[TermSymbol]] of this [[Tree]] */
      def term: TermSymbol = self.symbol.asTerm

      /** @return `true` if this [[Tree]] has a [[Symbol]] and it's a [[TermSymbol]] */
      def hasTerm: Boolean = hasSymbol && self.symbol.isTerm

      /**
       * Annotate this [[Tree]] with a specified [[Type]].
       * @param tpe The [[Type]] to use for this [[Tree]]
       * @return This [[Tree]] with its [[Type]] set
       */
      def withType(tpe: Type): Tree =
        setType(self, tpe.precise)

      /**
       * Annotate this [[Tree]] with a specified [[Type]].
       * @tparam T The [[Type]] to use for this [[Tree]]
       * @return This [[Tree]] with its [[Type]] set
       */
      def withType[T: TypeTag]: Tree =
        withType(typeOf[T])

      /**
       * Annotate this [[Tree]] with a specified [[Symbol]].
       * @param symbol The [[Symbol]] to use for this [[Tree]]
       * @return This [[Tree]] with its [[Symbol]] set
       */
      def withSym(symbol: Symbol): Tree = {
        val tree = setSymbol(self, symbol)
        if (symbol.hasType) tree withType symbol.preciseType else tree
      }

      /**
        * Annotate this [[Tree]] with a specified [[Position]].
        * @param position The [[Position]] to use for this [[Tree]]
        * @return This [[Tree]] with its [[Position]] set
        */
      def at(position: Position): Tree =
        atPos(position.makeTransparent)(self)

      /** Type-check this [[Tree]] if it doesn't have a [[Type]]. */
      def typeChecked: Tree = if (hasType) self else typeCheck(self)

      /** The [[Symbol]]s of all bound variables in this [[Tree]]. */
      def definitions: Set[TermSymbol] = typeChecked.collect {
        case defTree @ (_: DefDef | _: ValDef | _: Bind) => defTree
      }.filter { _.hasTerm }.map { _.term }.toSet

      /** The [[Symbol]]s of all variables referenced in this [[Tree]]. */
      def references: Set[TermSymbol] = typeChecked.collect {
        case id: Ident if id.hasTerm &&
          (id.term.isVal || id.term.isVar || id.term.isMethod) => id.term
      }.toSet

      /** The [[Symbol]]s of all free variables in this [[Tree]]. */
      def closure: Set[TermSymbol] = references diff definitions
    }
  }
}
