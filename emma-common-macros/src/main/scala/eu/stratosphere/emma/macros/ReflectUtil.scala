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
    def isTypeChecked: Boolean = self.tpe != null

    /** @return An un-type-checked version of this [[Tree]] */
    def unTypeChecked: Tree = unTypeCheck(self)

    /** @return `unTypeChecked.typeChecked` */
    def reTypeChecked: Tree = reTypeCheck(self)

    /** @return The most precise [[Type]] of this [[Tree]] */
    def trueType: Type = typeChecked.tpe.precise

    /** Type-check this [[Tree]] if it doesn't have a [[Type]]. */
    lazy val typeChecked: Tree = if (isTypeChecked) self else typeCheck(self)

    /** Collect the names of all bound variables in this [[Tree]]. */
    lazy val definitions: Set[TermName] = self.collect {
      case vd: ValDef              => vd.name
      case dd: DefDef              => dd.name
      case Bind(name: TermName, _) => name
    }.toSet

    /** Collect the names of all variables referenced in this [[Tree]]. */
    lazy val references: Set[TermName] = self.collect {
      case Ident(name: TermName) => name
    }.toSet

    /** Collect the names of all free variables in this [[Tree]]. */
    lazy val closure: Set[TermName] = self match {
      case Ident(name: TermName) => // reference
        Set(name)

      case Bind(name: TermName, body) => // case name => ...
        body.closure - name

      case ValDef(_, name, _, rhs) => // val name = ...
        rhs.closure - name

      case DefDef(_, name, _, args, _, rhs) => // def name(args: _*) = ...
        args.flatten.flatMap { _.closure }.toSet union (rhs.closure - name)

      case block: Block => // { ... }
        block.children.foldLeft(Set.empty[TermName], Set.empty[TermName]) {
          case ((bound, free), vd: ValDef) => // { val name = ... }
            val defined = if (isLazy(vd)) bound + vd.name else bound
            (bound + vd.name, free union vd.closure diff defined)

          case ((bound, free), dd: DefDef) => // { def name(args: _*) = ... }
            val defined = bound + dd.name
            (defined, free union dd.closure diff defined)

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
     * Replace a specified identifier in this [[Tree]] with a new one.
     *
     * @param name The [[TermName]] to replace
     * @param alias A new [[TermName]] to use in place of the old one
     * @return This [[Tree]] with the specified name replaced
     */
    def rename(name: TermName, alias: TermName): Tree =
      rename(name -> alias)

    /**
     * Rename a sequence of identifiers in this [[Tree]].
     *
     * @param aliases The sequence of identifiers to rename
     * @return This [[Tree]] with the specified names replaced
     */
    def rename(aliases: (TermName, TermName)*): Tree =
      rename(aliases.toMap)

    /**
     * Replace a [[Map]] of identifier in this [[Tree]] with their respective aliases.
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

      // def name(args: _*) = ...
      case DefDef(mods, name, typeArgs, args, tpt, rhs) if dict contains name =>
        val params = args map { _ map { _.rename(dict).as[ValDef] } }
        DefDef(mods, dict(name), typeArgs, params, tpt, rhs rename dict)
    }

    /**
     * Replace a specified identifier with a source code snippet in this [[Tree]].
     *
     * @param name The [[TermName]] to replace
     * @param value The source code [[Tree]] to substitute
     * @return This [[Tree]] with the substituted source code
     */
    def substitute(name: TermName, value: Tree): Tree =
      substitute(name -> value)

    /**
     * Replace a sequence of identifiers with their respective source code snippets in this
     * [[Tree]].
     *
     * @param kvs A sequence of [[TermName]]s and code snippets to substitute
     * @return This [[Tree]] with the substituted code snippets
     */
    def substitute(kvs: (TermName, Tree)*): Tree =
      substitute(kvs.toMap)

    /**
     * Replace a [[Map]] of identifiers with their respective source code snippets in this
     * [[Tree]].
     *
     * @param dict A dictionary of [[TermName]]s and code snippets to substitute
     * @return This [[Tree]] with the substituted code snippets
     */
    def substitute(dict: Map[TermName, Tree]): Tree = transform {
      // reference
      case Ident(name: TermName) if dict contains name =>
        dict(name)

      // case name => ...
      case Bind(name: TermName, body) if dict contains name =>
        Bind(termNames.WILDCARD, body substitute dict)

      // val name = ...
      case ValDef(_, name, _, _) => q"()"

      // def name(args: _*) = ...
      case DefDef(_, name, _, args, _, _)
        if args.flatten forall isImplicit => q"()"
    }

    /**
     * Inline a sequence of value definitions in this [[Tree]]. It's assumed that all of them are
     * part of the [[Tree]].
     *
     * @param defs the [[ValDef]]s to inline
     * @return this [[Tree]] with the specified value definitions inlined
     */
    def inline(defs: ValDef*): Tree =
      substitute((for (ValDef(_, name, _, rhs) <- defs) yield name -> rhs): _*)
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
    def apply(args: Type*): Type = appliedType(self, args: _*)
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
