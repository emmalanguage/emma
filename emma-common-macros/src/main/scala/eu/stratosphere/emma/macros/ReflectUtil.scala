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

  /**
   * Check if a value definition is lazy.
   *
   * @param vd The [[ValDef]] to check
   * @return `true` if the value definition is lazy, `false` otherwise
   */
  def isLazy(vd: ValDef): Boolean =
    vd.mods hasFlag Flag.LAZY

  /**
   * Check if a value definition (or parameter) is implicit.
   *
   * @param vd The [[ValDef]] to check
   * @return `true` if the value definition is implicit, `false` otherwise
   */
  def isImplicit(vd: ValDef): Boolean =
    vd.mods hasFlag Flag.IMPLICIT

  /** Generate a new [[TermName]]. */
  val freshName = internal.reificationSupport.freshTermName _

  /** Generate a new [[TypeName]]. */
  val freshType = internal.reificationSupport.freshTypeName _

  /** Remove all [[Type]] annotations from this [[Tree]]. */
  // FIXME: Replace with c.untypecheck once https://issues.scala-lang.org/browse/SI-5464 resolved.
  val unTypeCheck = showCode(_: Tree, printRootPkg = true) ->> parse

  /** Alias for `typeCheck compose unTypeCheck`. */
  val reTypeCheck = unTypeCheck andThen typeCheck

  /** Alias for `typeCheck compose parse`. */
  val parseCheck = typeCheck _ compose parse

  /** Syntactic sugar for [[Tree]]s. */
  class TreeOps(self: Tree) {

    private implicit def fromTree(tree: Tree): TreeOps =
      new TreeOps(tree)

    /** @return `true` if this [[Tree]] is annotated with a [[Type]], `false` otherwise */
    def hasType: Boolean = self.tpe != null

    /** @return `true` if this [[Tree]] is annotated with a [[Symbol]], `false` otherwise */
    def hasSymbol: Boolean = self.symbol != null

    /** @return An un-type-checked version of this [[Tree]] */
    def unTypeChecked: Tree = unTypeCheck(self)

    /** @return `unTypeChecked.typeChecked` */
    def reTypeChecked: Tree = reTypeCheck(self)

    /** @return The most precise [[Type]] of this [[Tree]] */
    def trueType: Type = {
      if (hasType) self.tpe
      else if (hasSymbol) self.symbol.info
      else typeChecked.tpe
    }.precise

    /** @return The [[Type]] argument used to initialize the [[Type]] if this [[Tree]] */
    def elementType: Type = trueType.typeArgs.head

    /** @return The [[TermSymbol]] of this [[Tree]] */
    def term: TermSymbol = self.symbol.asTerm

    /** @return `true` if this [[Tree]] has a [[Symbol]] and it's a [[TermSymbol]] */
    def hasTerm: Boolean = hasSymbol && self.symbol.isTerm

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
    lazy val closure: Set[TermName] = self match {
      case Ident(name: TermName) => // reference
        Set(name)

      case Bind(name: TermName, body) => // case name => ...
        body.closure - name

      case ValDef(_, name, _, rhs) => // val name = ...
        rhs.closure - name

      case Function(args, body) => // (args: _*) => ...
        body.closure diff args.map { _.name }.toSet

      case bl: Block => // { ... }
        bl.children.foldLeft(Set.empty[TermName], Set.empty[TermName]) {
          case ((bound, free), vd: ValDef) => // { val name = ... }
            val defined = if (isLazy(vd)) bound + vd.name else bound
            (bound + vd.name, free union vd.closure diff defined)

          case ((bound, free), child) => // default
            (bound, free union child.closure diff bound)
        }._2

      case tree => // default
        tree.children.flatMap { _.closure }.toSet
    }

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
      rename((for (n <- names) yield n -> freshName(s"$n$$")): _*)

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
          case vd @ ValDef(_, `key`, _, _) => isLazy(vd)
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
      case vd: ValDef if vd.symbol == valDef.symbol => q"()"
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

  /** Syntax sugar for function call chain emulation. */
  implicit class ApplyWith[A](self: A) {

    /**
     * Emulation of a function call chain starting with `this`, similar to Clojure's thread macros
     * (-> and/or ->>). E.g. `x ->> f ->> g == g(f(x)) == (g compose f)(x)`.
     *
     * @param f Next function to thread in the call chain
     * @tparam B Return type of the next function
     */
    def ->>[B](f: A => B) = f(self)
  }

  /** Syntax sugar for subclass testing and class casting. */
  implicit class InstanceOf[A](self: A) {
    
    /** Alias of `asInstanceOf[B]`. */
    def as[B]: B = self.asInstanceOf[B]
  }
}
