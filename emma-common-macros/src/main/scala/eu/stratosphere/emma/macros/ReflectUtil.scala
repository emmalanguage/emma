package eu.stratosphere.emma.macros

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

  lazy val UNIT    = typeOf[Unit]
  lazy val BOOL    = typeOf[Boolean]
  lazy val CHAR    = typeOf[Char]
  lazy val BYTE    = typeOf[Byte]
  lazy val SHORT   = typeOf[Short]
  lazy val INT     = typeOf[Int]
  lazy val LONG    = typeOf[Long]
  lazy val FLOAT   = typeOf[Float]
  lazy val DOUBLE  = typeOf[Double]
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

  lazy val PAIR  = TUPLE(2)
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
  def termSym(
      owner: Symbol,
      name:  TermName,
      tpe:   Type,
      flags: FlagSet  = Flag.SYNTHETIC,
      pos:   Position = NoPosition): TermSymbol

  /** Create a new [[TypeSymbol]]. */
  def typeSym(
     owner: Symbol,
     name:  TypeName,
     flags: FlagSet  = Flag.SYNTHETIC,
     pos:   Position = NoPosition): TypeSymbol

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

    def ref(sym: TermSymbol): Ident =
      mkIdent(sym).withType(sym.info).as[Ident]

    def bind(sym: TermSymbol, rhs: Tree = EmptyTree): Bind =
      Bind(sym.name, rhs).withSym(sym).as[Bind]

    def freeTerm(
        name:   String,
        tpe:    Type,
        flags:  FlagSet = Flag.SYNTHETIC,
        origin: String  = null): TermSymbol =
      newFreeTerm(name, null, flags, origin).withInfo(tpe).asTerm

    def freeType(
        name:   String,
        flags:  FlagSet = Flag.SYNTHETIC,
        origin: String  = null): TypeSymbol =
      newFreeType(name, flags, origin)

    def valDef(sym: TermSymbol): ValDef =
      valDef(sym, EmptyTree)

    def valDef(sym: TermSymbol, rhs: Tree): ValDef =
      internal.valDef(sym, rhs).withType(sym.info).as[ValDef]

    def valDef(name: TermName, tpt: Tree): ValDef =
      valDef(name, tpt, EmptyTree)

    def valDef(name: TermName, tpt: Tree, rhs: Tree): ValDef =
      valDef(name, tpt, Flag.SYNTHETIC, rhs)

    def valDef(name: TermName, tpt: Tree, flags: FlagSet, rhs: Tree): ValDef =
      ValDef(Modifiers(flags), name, tpt, rhs).withType(tpt.trueType).as[ValDef]

    def valDef(
        name:  TermName,
        tpe:   Type,
        owner: Symbol   = NoSymbol,
        flags: FlagSet  = Flag.SYNTHETIC,
        pos:   Position = NoPosition,
        rhs:   Tree     = EmptyTree): ValDef =
      valDef(termSym(owner, name, tpe, flags, pos).asTerm, rhs)

    def anonFun(
        args:  List[(TermName, Type)],
        body:  Tree,
        owner: Symbol   = NoSymbol,
        flags: FlagSet  = Flag.SYNTHETIC,
        pos:   Position = NoPosition): Function = {
      val tpe    = FUN(args.size)(args.map { _._2 } :+ body.trueType: _*)
      val sym    = termSym(owner, TermName("anonfun"), tpe, flags, pos)
      val params = for ((name, tpe) <- args)
        yield valDef(name, tpe, sym, Flag.SYNTHETIC | Flag.PARAM, pos)

      Function(params, body).withSym(sym).as[Function]
    }

    def select(sym: Symbol, apply: Boolean): Tree = {
      if (sym.owner != rootMirror.RootClass) {
        val name = if (!apply && !sym.isPackage)
          sym.name.toTypeName else sym.name.toTermName

        Select(select(sym.owner, apply), name) withSym sym
      } else ref(rootMirror staticPackage sym.fullName)
    }
  }

  /** Syntactic sugar for [[Tree]]s. */
  class TreeOps(self: Tree) {

    /** @return `true` if this [[Tree]] is annotated with a [[Type]], `false` otherwise */
    def hasType: Boolean = {
      val tpe = self.tpe
      tpe != null && tpe != NoType
    }

    /** @return `true` if this [[Tree]] is annotated with a [[Symbol]], `false` otherwise */
    def hasSymbol: Boolean = {
      val sym = self.symbol
      sym != null && sym != NoSymbol
    }

    /** @return `true` if this [[Tree]] has an owner, `false` otherwise */
    def hasOwner: Boolean = hasSymbol && {
      val owner = self.symbol.owner
      owner != null && owner != NoSymbol
    }

    /** @return This [[Tree]]'s owner */
    def owner: Symbol = self.symbol.owner

    /** @return An un-type-checked version of this [[Tree]] */
    def unTypeChecked: Tree = unTypeCheck(self)

    /** @return `unTypeChecked.typeChecked` */
    def reTypeChecked: Tree = reTypeCheck(self)

    /** @return The most precise [[Type]] of this [[Tree]] */
    def trueType: Type = {
      if (hasType) self.tpe
      else if (hasSymbol && self.symbol.hasInfo) self.symbol.info
      else typeChecked.tpe
    }.precise

    /** @return The [[Type]] argument used to initialize the [[Type]] if this [[Tree]] */
    def elementType: Type = trueType.typeArgs.head

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
      setType(self, tpe)

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
     * @param sym The [[Symbol]] to use for this [[Tree]]
     * @return This [[Tree]] with its [[Symbol]] set
     */
    def withSym(sym: Symbol): Tree = {
      val tree = setSymbol(self, sym)
      if (sym.hasInfo) tree withType sym.info else tree
    }

    /** Type-check this [[Tree]] if it doesn't have a [[Type]]. */
    lazy val typeChecked: Tree = if (hasType) self else typeCheck(self)

    /** Collect the [[Symbol]]s of all bound variables in this [[Tree]]. */
    lazy val definitions: Set[TermSymbol] = typeChecked.collect {
      case vd: ValDef if vd.hasTerm => vd.term
      case bd: Bind   if bd.hasTerm => bd.term
    }.toSet

    /** Collect the [[Symbol]]s of all variables referenced in this [[Tree]]. */
    lazy val references: Set[TermSymbol] = typeChecked.collect {
      case id: Ident if id.hasTerm && (id.term.isVal || id.term.isVar) => id.term
    }.toSet

    /** Collect the [[Symbol]]s of all free variables in this [[Tree]]. */
    lazy val freeTerms: Set[TermSymbol] = references diff definitions

    /** Collect the [[TermName]]s of all free variables in this [[Tree]]. */
    lazy val closure: Set[TermName] = freeTerms map { _.name }

    /**
     * Recursively apply a depth-first transformation to this [[Tree]].
     *
     * @param pf A [[PartialFunction]] to transform some of the [[Tree]] nodes
     * @return A new [[Tree]] with some of the nodes transformed
     */
    def transform(pf: Tree ~> Tree): Tree = new Transformer {
      override def transform(tree: Tree) =
        if (pf isDefinedAt tree) pf(tree)
        else super.transform(tree)
    } transform self

    /**
     * Bind a name to a value in this [[Tree]].
     * 
     * @param name The [[TermName]] to bind
     * @param value The [[Tree]] to bind the name to
     * @return This [[Tree]] with `name` bound to `value`
     */
    def bind(name: TermName, value: Tree): Tree =
      bind(name -> value)

    /**
     * Bind a [[Map]] of name-value pairs in this [[Tree]].
     *
     * @param dict A dictionary of name-value pairs to bind
     * @return This [[Tree]] with all names in `dict` bound to their respective values
     */
    def bind(dict: Map[TermName, Tree]): Tree =
      bind(dict.toSeq: _*)

    /**
     * Bind a sequence of name-value pairs in this [[Tree]].
     *
     * @param bindings A sequence of name-value pairs to bind
     * @return This [[Tree]] with all names in `kvs` bound to their respective values
     */
    def bind(bindings: (TermName, Tree)*): Tree =
      q"{ ..${for ((k, v) <- bindings) yield q"val $k = $v"}; $self }"


    /**
     * Replace a sequence of identifiers in this [[Tree]] with fresh [[TermName]]s.
     *
     * @param names The sequence of identifiers to rename
     * @return This [[Tree]] with the specified names replaced
     */
    def refresh(names: TermName*): Tree =
      rename((for (n <- names) yield n -> freshName(n.toString)): _*)

    /**
     * Replace a specified identifier in this [[Tree]] with a new one.
     *
     * @param key The [[TermName]] to replace
     * @param alias A new [[TermName]] to use in place of the old one
     * @return This [[Tree]] with the specified name replaced
     */
    def rename(key: TermName, alias: TermName): Tree =
      rename(key -> alias)

    /**
     * Replace a sequence of identifiers in this [[Tree]] with their respective aliases.
     *
     * @param aliases The sequence of identifiers to rename
     * @return This [[Tree]] with the specified names replaced
     */
    def rename(aliases: (TermName, TermName)*): Tree =
      rename(aliases.toMap)

    /**
     * Replace a [[Map]] of identifiers in this [[Tree]] with their respective aliases.
     *
     * @param dict A dictionary of aliases to be renames
     * @return This [[Tree]] with the specified names replaced
     */
    def rename(dict: Map[TermName, TermName]): Tree = transform {
      // reference
      case Ident(name: TermName) if dict contains name =>
        Ident(dict(name))

      // case name => ...
      case Bind(name: TermName, body) if dict contains name =>
        Bind(dict(name), body rename dict)

      // val name = ...
      case ValDef(mods, name, tpt, rhs) if dict contains name =>
        ValDef(mods, dict(name), tpt, rhs rename dict)
    }

    /**
     * Replace all free occurrences of an identifier with a source code snippet in this [[Tree]].
     *
     * @param key The [[TermName]] to replace
     * @param value The source code [[Tree]] to substitute
     * @return This [[Tree]] with the substituted source code
     */
    def substitute(key: TermName, value: Tree): Tree = {
      val closure = value.closure
      transform {
        // reference
        case Ident(`key`) => value
        case Typed(Ident(`key`), _) => value

        // case name => ...
        case bd @ Bind(`key`, _) => bd
        case bd @ Bind(name: TermName, _) if closure(name) =>
          bd.refresh(name).substitute(key, value)

        // val name = ...
        case vd @ ValDef(_, `key`, _, _) => vd
        case vd @ ValDef(_, name, _, _) if closure(name) =>
          vd.refresh(name).substitute(key, value)

        // (args: _*) => ...
        case fn @ Function(args, _) if args exists { _.name == key } => fn
        case fn @ Function(args, _) if args exists { arg => closure(arg.name) } =>
          fn.refresh(args map { _.name } filter closure: _*).substitute(key, value)

        // { ... lazy val name = ... }
        case bl: Block if bl.children exists {
          case vd @ ValDef(_, `key`, _, _) => vd.isLazy
          case _ => false
        } => bl

        // { ... }
        case bl: Block if bl.children exists {
          case vd @ ValDef(_, name, _, _) => closure(name)
          case _ => false
        } => bl.refresh(bl.children collect {
          case vd @ ValDef(_, name, _, _) if closure(name) => name
        }: _*).substitute(key, value)

        // { ... val name = ... }
        case bl: Block if bl.children exists {
          case vd: ValDef => vd.name == key
          case _ => false
        } => bl.children span {
          case vd: ValDef => vd.name != key
          case _ => true
        } match { case (pre, post) =>
          Block(pre.map { _.substitute(key, value) } ::: post.init, post.last)
        }
      }
    }

    /**
     * Replace all free occurrences of a sequence of identifiers with their respective source code
     * snippets in this [[Tree]].
     *
     * @param kvs A sequence of [[TermName]]s and code snippets to substitute
     * @return This [[Tree]] with the substituted code snippets
     */
    def substitute(kvs: (TermName, Tree)*): Tree =
      kvs.foldLeft(self) { case (tree, (key, value)) => tree.substitute(key, value) }

    /**
     * Replace all free occurrences of a [[Map]] of identifiers with their respective source code
     * snippets in this [[Tree]].
     *
     * @param dict A dictionary of [[TermName]]s and code snippets to substitute
     * @return This [[Tree]] with the substituted code snippets
     */
    def substitute(dict: Map[TermName, Tree]): Tree =
      substitute(dict.toSeq: _*)

    /**
     * Inline a value definitions in this [[Tree]]. It's assumed that it's part of the [[Tree]].
     *
     * @param valDef the [[ValDef]] to inline
     * @return this [[Tree]] with the specified value definitions inlined
     */
    def inline(valDef: ValDef): Tree = transform {
      case vd: ValDef if vd.symbol == valDef.symbol => q"()".withType[Unit]
      case id: Ident  if id.symbol == valDef.symbol => valDef.rhs
    }

    /**
     * Create a function that replaces all occurrences of identifiers from the given environment with
     * fresh identifiers. This can be used to "free" identifiers from their original [[Symbol]]s.
     *
     * @param vars An environment consisting of a list of [[ValDef]]s
     * @return A function that can "free" the environment of a [[Tree]]
     */
    def freeEnv(vars: ValDef*): Tree = {
      val varSet = vars.map { _.name }.toSet
      transform { case Ident(name: TermName) if varSet(name) => Ident(name) }
    }
  }

  /** Syntax sugar for [[ValDef]]s. */
  implicit class ValDefOps(self: ValDef) {

    /** @return `true` if the value definition is lazy, `false` otherwise */
    def isLazy: Boolean = self.mods hasFlag Flag.LAZY

    /** @return `true` if the value definition is implicit, `false` otherwise */
    def isImplicit: Boolean = self.mods hasFlag Flag.IMPLICIT

    /** @return This [[ValDef]] without a [[Symbol]] and a `rhs` */
    def reset: ValDef = mk.valDef(self.name, self.tpt, self.mods.flags, EmptyTree)
  }

  /** Syntax sugar for [[Type]]s. */
  implicit class TypeOps(self: Type) {

    /** @return The de-aliased and widened version of this [[Type]] */
    def precise: Type = self.finalResultType.widen

    /** @return The [[Type]] argument used to initialize this [[Type]] constructor */
    def elementType: Type = precise.typeArgs.head

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
    def hasInfo: Boolean = {
      val info = self.info
      info != null && info != NoType
    }

    /** Set the `info` of this [[Symbol]]. */
    def withInfo(info: Type): Symbol =
      setInfo(self, info)

    /** Set the `info` of this [[Symbol]]. */
    def withInfo[T: TypeTag]: Symbol =
      withInfo(typeOf[T])
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
