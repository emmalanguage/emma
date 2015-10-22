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

  /** Alias of [[PartialFunction]]. */
  type ~>[A, B] = PartialFunction[A, B]

  /**
   * Parse a [[String]] of source code.
   *
   * @param str The source code to parse
   * @return A [[Tree]] representation of the source code
   */
  def parse(str: String): Tree

  /**
   * Type-check a source code snippet.
   *
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

  /** Generate a new [[TermName]]. */
  def freshName(prefix: String): TermName =
    if (prefix.nonEmpty && prefix.last == '$') freshTermName(prefix)
    else freshTermName(s"$prefix$$")

  /** Generate a new [[TypeName]]. */
  def freshType(prefix: String): TypeName =
    if (prefix.nonEmpty && prefix.last == '$') freshTypeName(prefix)
    else freshTypeName(s"$prefix$$")

  /** Remove all [[Type]] annotations from this [[Tree]]. */
  // FIXME: Replace with c.untypecheck once https://issues.scala-lang.org/browse/SI-5464 resolved
  val unTypeCheck = { showCode(_: Tree, printRootPkg = true) } andThen parse

  /** Alias for `typeCheck compose unTypeCheck`. */
  val reTypeCheck = unTypeCheck andThen typeCheck

  /** Alias for `typeCheck compose parse`. */
  val parseCheck = parse _ andThen typeCheck

  /** Syntax sugar for [[Tree]]s. */
  private implicit def fromTree(tree: Tree): TreeOps =
    new TreeOps(tree)

  /** [[Tree]] constructors. */
  object mk {

    def ref(term: TermSymbol): Ident =
      Ident(term.name).withSym(term).as[Ident]

    def bind(term: TermSymbol,
        rhs: Tree = Ident(termNames.WILDCARD)): Bind =
      Bind(term.name, rhs).withSym(term).as[Bind]

    def freeTerm(name: String, tpe: Type,
        flags: FlagSet = Flag.SYNTHETIC,
        origin: String = null): TermSymbol =
      newFreeTerm(name, null, flags, origin).withType(tpe.precise).asTerm

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
      valDef(termSym(owner, name, tpe.precise, flags, pos).asTerm, rhs)

    def anonFun(args: List[TermSymbol], body: Tree,
        owner: Symbol = NoSymbol,
        flags: FlagSet = Flag.SYNTHETIC,
        pos: Position = NoPosition): Function = {
      val argTypes = args.map { _.preciseType }
      val funType = FUN(args.size)(argTypes :+ body.preciseType: _*)
      val funSym = termSym(owner, TermName("anonfun"), funType, flags, pos)
      val params = for ((arg, tpe) <- args zip argTypes)
        yield valDef(arg.name, tpe, funSym, Flag.SYNTHETIC | Flag.PARAM, pos)

      val tree = body.rename(args zip params.map { _.term }: _*)
      Function(params, tree).withSym(funSym).as[Function]
    }

    def select(sym: Symbol): Tree =
      if (Set(null, NoSymbol, rootMirror.RootClass)(sym.owner))
        ref(rootMirror.staticPackage(sym.fullName))
      else q"${select(sym.owner)}.${sym.name.toTermName}" withSym sym

    def typeSelect(sym: Symbol): Tree =
      if (Set(null, NoSymbol, rootMirror.RootClass)(sym.owner))
        ref(rootMirror.staticPackage(sym.fullName))
      else tq"${select(sym.owner)}.${sym.name.toTypeName}" withSym sym
  }

  /** Syntactic sugar for [[Tree]]s. */
  class TreeOps(self: Tree) {

    /** @return `true` if this [[Tree]] is annotated with a [[Type]], `false` otherwise */
    def hasType: Boolean = self.tpe != null && self.tpe != NoType

    /** @return `true` if this [[Tree]] is annotated with a [[Symbol]], `false` otherwise */
    def hasSymbol: Boolean = self.symbol != null && self.symbol != NoSymbol

    /** @return `true` if this [[Tree]] has an owner, `false` otherwise */
    def hasOwner: Boolean = hasSymbol &&
      self.symbol.owner != null && self.symbol.owner != NoSymbol

    /** @return This [[Tree]]'s owner */
    def owner: Symbol = self.symbol.owner

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
     *
     * @param tpe The [[Type]] to use for this [[Tree]]
     * @return This [[Tree]] with its [[Type]] set
     */
    def withType(tpe: Type): Tree =
      setType(self, tpe.precise)

    /**
     * Annotate this [[Tree]] with a specified [[Type]].
     *
     * @tparam T The [[Type]] to use for this [[Tree]]
     * @return This [[Tree]] with its [[Type]] set
     */
    def withType[T: TypeTag]: Tree =
      withType(typeOf[T])

    /**
     * Annotate this [[Tree]] with a specified [[Symbol]].
     *
     * @param symbol The [[Symbol]] to use for this [[Tree]]
     * @return This [[Tree]] with its [[Symbol]] set
     */
    def withSym(symbol: Symbol): Tree = {
      val tree = setSymbol(self, symbol)
      if (symbol.hasType) tree withType symbol.preciseType else tree
    }

    /** Type-check this [[Tree]] if it doesn't have a [[Type]]. */
    lazy val typeChecked: Tree = if (hasType) self else typeCheck(self)

    /** The [[Symbol]]s of all bound variables in this [[Tree]]. */
    lazy val definitions: Set[TermSymbol] = typeChecked.collect {
      case defTree @ (_: DefDef | _: ValDef | _: Bind) => defTree
    }.filter { _.hasTerm }.map { _.term }.toSet

    /** The [[Symbol]]s of all variables referenced in this [[Tree]]. */
    lazy val references: Set[TermSymbol] = typeChecked.collect {
      case id: Ident if id.hasTerm &&
        (id.term.isVal || id.term.isVar || id.term.isMethod) => id.term
    }.toSet

    /** The [[Symbol]]s of all free variables in this [[Tree]]. */
    lazy val closure: Set[TermSymbol] = references diff definitions

    /**
     * Recursively apply a depth-first transformation to this [[Tree]].
     *
     * @param pf A [[PartialFunction]] to transform some of the [[Tree]] nodes
     * @return A new [[Tree]] with some of the nodes transformed
     */
    def transform(pf: Tree ~> Tree): Tree = new Transformer {
      override def transform(tree: Tree): Tree =
        if (pf.isDefinedAt(tree)) pf(tree)
        else super.transform(tree)
    }.transform(self)

    /**
     * Recursively apply a depth-first traversal to this [[Tree]].
     *
     * @param pf A [[PartialFunction]] to traverse some of the [[Tree]] nodes
     */
    def traverse(pf: Tree ~> Unit): Unit = new Traverser {
      override def traverse(tree: Tree): Unit =
        if (pf.isDefinedAt(tree)) pf(tree)
        else super.traverse(tree)
    }.traverse(self)

    /**
     * Recursively remove all layers of type ascriptions from this [[Tree]].
     * E.g. `q"((42: Int): Int)".unAscribed = q"42"`
     *
     * @return An equivalent [[Tree]] without type ascriptions.
     */
    @tailrec final def unAscribed: Tree = self match {
      case q"${tree: Tree}: $_" => tree.unAscribed
      case _ => self
    }

    /**
     * Bind a [[Name]] to a value in this [[Tree]].
     * 
     * @param name The [[TermName]] to bind
     * @param value The [[Tree]] to bind the name to
     * @return This [[Tree]] with `name` bound to `value`
     */
    def bind(name: TermName, value: Tree): Tree =
      bind(name -> value)

    /**
     * Bind a sequence of name-value pairs in this [[Tree]].
     *
     * @param bindings A sequence of name-value pairs to bind
     * @return This [[Tree]] with all names in `kvs` bound to their respective values
     */
    def bind(bindings: (TermName, Tree)*): Tree =
      if (bindings.isEmpty) self else
        q"{ ..${for ((k, v) <- bindings) yield q"val $k = $v"}; $self }"

    /**
     * Bind a [[Symbol]] to a value in this [[Tree]].
     *
     * @param term The [[Symbol]] to bind
     * @param value The [[Tree]] to bind the name to
     * @return This [[Tree]] with `term` bound to `value`
     */
    def bind(term: TermSymbol, value: Tree): Tree =
      bind(Map(term -> value))

    /**
     * Bind a dictionary of [[Symbol]]-value pairs in this [[Tree]].
     *
     * @param dict A [[Map]] of [[Symbol]]-value pairs to bind
     * @return This [[Tree]] with all [[Symbol]]s in `dict` bound to their respective values
     */
    def bind(dict: Map[TermSymbol, Tree]): Tree =
      if (dict.isEmpty) self else {
        val bindings = dict filterKeys closure map { case (k, v) =>
          val name = freshName(k.name.toString)
          (k: Symbol, mk.valDef(name, k.preciseType, k.owner, pos = k.pos, rhs = v))
        }

        val body = self.rename(bindings mapValues { _.term })
        q"{ ..${bindings.values}; $body }" withType preciseType
      }

    /**
     * Replace occurrences of the `find` [[Tree]] with the `replacement` [[Tree]].
     *
     * @param find The [[Tree]] to look for
     * @param replacement The [[Tree]] that should replace `find`
     * @return A substituted version of the enclosing [[Tree]]
     */
    def substitute(find: Tree, replacement: Tree): Tree =
      transform { case `find` => replacement }

    @deprecated("DANGER: Use `rename(aliases: (Symbol, TermSymbol)*)` instead.", "0.1.0")
    def refresh_!(names: TermName*): Tree =
      rename_!((for (n <- names) yield n -> freshName(n.toString)).toMap)

    @deprecated("DANGER: Use `rename(dict: Map[Symbol, TermName])` instead.", "0.1.0")
    def rename_!(dict: Map[TermName, TermName]): Tree = transform {
      // reference
      case Ident(name: TermName) if dict contains name =>
        Ident(dict(name))

      // case name => ...
      case Bind(name: TermName, body) if dict contains name =>
        Bind(dict(name), body rename_! dict)

      // val name = ...
      case ValDef(mods, name, tpt, rhs) if dict contains name =>
        ValDef(mods, dict(name), tpt, rhs rename_! dict)
    }

    @deprecated("""DANGER: Think carefully!
    | Usually you can use a combination of `bind` and `rename` instead.
    | Consider the following example: `(x + 5).substitute("x", 42) == { val x = 42; x + 5 }`
    |""".stripMargin, "0.1.0")
    def substitute_!(key: TermName, value: Tree): Tree = {
      val closure = value.closure map { _.name }
      transform {
        // reference
        case Ident(`key`) => value
        case Typed(Ident(`key`), _) => value

        // case name => ...
        case bd @ Bind(`key`, _) => bd
        case bd @ Bind(name: TermName, _) if closure(name) =>
          bd.refresh_!(name).substitute_!(key, value)

        // val name = ...
        case vd @ ValDef(_, `key`, _, _) => vd
        case vd @ ValDef(_, name, _, _) if closure(name) =>
          vd.refresh_!(name).substitute_!(key, value)

        // (args: _*) => ...
        case fn @ Function(args, _) if args exists { _.name == key } => fn
        case fn @ Function(args, _) if args exists { arg => closure(arg.name) } =>
          fn.refresh_!(args map { _.name } filter closure: _*).substitute_!(key, value)

        // { ... lazy val name = ... }
        case bl: Block if bl.children exists {
          case vd @ ValDef(_, `key`, _, _) => vd.mods hasFlag Flag.LAZY
          case _ => false
        } => bl

        // { ... }
        case bl: Block if bl.children exists {
          case vd @ ValDef(_, name, _, _) => closure(name)
          case _ => false
        } => bl.refresh_!(bl.children collect {
          case vd @ ValDef(_, name, _, _) if closure(name) => name
        }: _*).substitute_!(key, value)

        // { ... val name = ... }
        case bl: Block if bl.children exists {
          case vd: ValDef => vd.name == key
          case _ => false
        } => bl.children span {
          case vd: ValDef => vd.name != key
          case _ => true
        } match { case (pre, post) =>
          Block(pre.map { _.substitute_!(key, value) } ::: post.init, post.last)
        }
      }
    }

    /**
     * Replace a dictionary of [[Symbol]]s with references to their aliases.
     * 
     * @param dict A [[Map]] of aliases to replace
     * @return This [[Tree]] with the specified [[Symbol]]s replaced 
     */
    def rename(dict: Map[Symbol, TermSymbol]): Tree =
      if (dict.isEmpty) self else
        transform { case id: Ident if dict contains id.symbol => mk ref dict(id.symbol) }

    /**
     * Replace a sequence of [[Symbol]]s with references to their aliases.
     *
     * @param aliases A sequence of aliases to replace
     * @return This [[Tree]] with the specified [[Symbol]]s replaced
     */
    def rename(aliases: (Symbol, TermSymbol)*): Tree =
      rename(aliases.toMap)

    /**
     * Replace a [[Symbol]]s with a reference to an alias.
     *
     * @param key The [[Symbol]] to rename
     * @param alias An alternative [[Symbol]] to use
     * @return This [[Tree]] with the specified [[Symbol]] replaced
     */
    def rename(key: Symbol, alias: TermSymbol): Tree =
      rename(key -> alias)

    /**
     * Inline a value definitions in this [[Tree]]. It's assumed that it's part of the [[Tree]].
     *
     * @param valDef the [[ValDef]] to inline
     * @return this [[Tree]] with the specified value definitions inlined
     */
    def inline(valDef: ValDef): Tree = transform {
      case vd: ValDef if vd.symbol == valDef.symbol => q"()".withType[Unit]
      case id: Ident if id.symbol == valDef.symbol => valDef.rhs
    }
  }

  /** Syntax sugar for [[Type]]s. */
  implicit class TypeOps(self: Type) {

    /** @return The de-aliased and widened version of this [[Type]] */
    def precise: Type = self.finalResultType.widen

    /**
     * Apply this [[Type]] as a [[Type]] constructor.
     *
     * @param args The [[Type]] arguments
     * @return The specialized version of this [[Type]]
     */
    def apply(args: Type*): Type =
      appliedType(self, args: _*)
  }

  /** Syntax sugar for [[Symbol]]s. */
  implicit class SymbolOps(self: Symbol) {

    /** Check if this [[Symbol]] has an associated [[Type]]. */
    def hasType: Boolean = self.info != null && self.info != NoType

    /** Set the `info` of this [[Symbol]]. */
    def withType(tpe: Type): Symbol = setInfo(self, tpe.precise)

    /** Set the `info` of this [[Symbol]]. */
    def withType[T: TypeTag]: Symbol = withType(typeOf[T])

    /** @return The most precise [[Type]] of this [[Symbol]] */
    def preciseType: Type = self.info.precise
  }

  /** Syntax sugar for function call chain emulation. */
  implicit class Chain[A](self: A) {

    /**
     * Emulation of a function call chain starting with `this`, similar to Clojure's thread macros
     * (-> and/or ->>). E.g. `x ->> f ->> g == g(f(x)) == (g compose f)(x) == (f andThen g)(x)`.
     *
     * @param f Next function to thread in the call chain
     * @tparam B Return type of the next function
     */
    def ->>[B](f: A => B): B = f(self)

    /** Alias for `this ->> f`. */
    def chain[B](f: A => B): B = f(self)
  }

  /** Syntax sugar for class casting. */
  implicit class As[A](self: A) {
    /** Alias for `this.asInstanceOf[B]`. */
    def as[B]: B = self.asInstanceOf[B]
  }
}
