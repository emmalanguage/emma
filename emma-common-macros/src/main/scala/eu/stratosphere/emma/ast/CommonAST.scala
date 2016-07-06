package eu.stratosphere
package emma.ast

import emma.compiler.ReflectUtil

import scala.annotation.tailrec
import scala.reflect.api.Universe
import scala.reflect.macros.Attachments

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros and runtime reflection APIs, e.g. non-idempotent type checking, lack of hygiene,
 * capture-avoiding substitution, fully-qualified names, fresh name generation, identifying
 * closures, etc.
 *
 * This trait has to be instantiated with a [[scala.reflect.api.Universe]] type and works for both
 * runtime and compile time reflection.
 */
trait CommonAST extends ReflectUtil {

  val universe: Universe
  lazy val u: universe.type = universe

  import universe._
  import internal._
  import decorators._
  import reificationSupport._

  /** Raises an error and terminates compilation. */
  def abort(msg: String, pos: Position = NoPosition): Nothing

  /** Raises a compiler warning. */
  def warning(msg: String, pos: Position = NoPosition): Unit

  /** Constant limits. */
  object Max {

    /** Maximum number of lambda arguments. */
    val FunParams = definitions.FunctionClass.seq.size

    /** Maximum number of tuple elements. */
    val TupleElems = definitions.TupleClass.seq.size
  }

  // ---------------------------
  // Parsing and type-checking
  // ---------------------------

  /** Parses a snippet of source code and returns the AST. */
  private[emma] override def parse(code: String): Tree

  /** Type-checks a `tree` (use `typeMode=true` for type-trees). */
  private[emma] override def typeCheck(tree: Tree, typeMode: Boolean = false): Tree

  /** Removes all type and symbol attributes from a `tree`. */
  // FIXME: Replace with `c.untypecheck` or `tb.untypecheck` once SI-5464 is resolved.
  private[emma] override def unTypeCheck(tree: Tree): Tree =
    parse(showCode(tree, printRootPkg = true))

  // ------------------------------
  // Abstract getters and setters
  // ------------------------------

  /** Getter for the attributes of various compilation entities. */
  val get: Getter

  /**
   * Setter for the attributes of various compilation entities.
   * WARN: Mutates in place.
   */
  private[ast] val set: Setter

  /** Getter for the attributes of various compilation entities. */
  trait Getter {

    // Symbols
    def enclosingOwner: Symbol
    def ans(sym: Symbol): Seq[Annotation] = sym.annotations
    def flags(sym: Symbol): FlagSet = sym.flags
    def meta(sym: Symbol): Attachments
    def name(sym: Symbol): Name = sym.name
    def owner(sym: Symbol): Symbol = sym.owner
    def pos(sym: Symbol): Position = sym.pos
    def tpe(sym: Symbol): Type = sym.info

    // Trees
    def meta(tree: Tree): Attachments
    def original(tpt: TypeTree): Tree = tpt.original
    def pos(tree: Tree): Position = tree.pos
    def sym(tree: Tree): Symbol = tree.symbol
    def tpe(tree: Tree): Type = tree.tpe

    // Types
    def pos(tpe: Type): Position =
      if (has.typeSym(tpe)) tpe.typeSymbol.pos
      else if (has.termSym(tpe)) tpe.termSymbol.pos
      else NoPosition
  }

  /**
   * Setter for the attributes of various compilation entities.
   * WARN: Mutates in place.
   */
  private[ast] trait Setter {

    /** Set multiple properties of a `tree` at once. */
    def apply(tree: Tree,
      pos: Position = NoPosition,
      sym: Symbol = null,
      tpe: Type = NoType): Unit = {

      if (pos != null) set.pos(tree, pos)
      if (sym != null) set.sym(tree, sym)
      if (tpe != null) set.tpe(tree, tpe)
    }

    // Symbols
    def ans(sym: Symbol, ans: Annotation*): Unit = setAnnotations(sym, ans.toList)
    def flags(sym: Symbol, flags: FlagSet): Unit
    def name(sym: Symbol, name: Name): Unit
    def owner(sym: Symbol, owner: Symbol): Unit
    def pos(sym: Symbol, pos: Position): Unit
    def tpe(sym: Symbol, tpe: Type): Unit = setInfo(sym, tpe)

    // Trees
    def original(tpt: TypeTree, original: Tree): Unit
    def pos(tree: Tree, pos: Position): Unit
    def sym(tree: Tree, sym: Symbol): Unit = setSymbol(tree, sym)
    def tpe(tree: Tree, tpe: Type): Unit = setType(tree, tpe)
  }

  // ------------
  // Extractors
  // ------------
  // scalastyle:off

  /** Extractor for the name of a symbol, if any. */
  object withName {
    def unapply(sym: Symbol): Option[(Symbol, Name)] =
      if (has.name(sym)) Some(sym, sym.name) else None
  }

  /** Extractor for the symbol of a tree, if any. */
  object withSym {
    def unapply(tree: Tree): Option[(Tree, Symbol)] =
      if (has.sym(tree)) Some(tree, tree.symbol) else None
  }

  /** Extractor for the type of a tree, if any. */
  object withType {
    def unapply(tree: Tree): Option[(Tree, Type)] =
      if (has.tpe(tree)) Some(tree, tree.tpe.dealias.widen) else None
  }

  // scalastyle:on
  // ---------------
  // Property checks
  // ---------------

  object is {

    /** Does `sym` have the property encoded as flag(s)? */
    def apply(property: FlagSet)(sym: Symbol): Boolean =
      are(property)(sym.flags)

    /** The opposite of `is(property)(sym)`. */
    def not(property: FlagSet)(sym: Symbol): Boolean =
      are.not(property)(sym.flags)

    /** Is `name` non-degenerate? */
    def defined(name: Name): Boolean =
      name != null && name.toString.nonEmpty

    /** Is `pos` non-degenerate? */
    def defined(pos: Position): Boolean =
      pos != null && pos != NoPosition

    /** Is `sym` non-degenerate? */
    def defined(sym: Symbol): Boolean =
      sym != null && sym != NoSymbol

    /** Is `tree` non-degenerate? */
    def defined(tree: Tree): Boolean =
      tree != null && tree.nonEmpty

    /** Is `tpe` non-degenerate? */
    def defined(tpe: Type): Boolean =
      tpe != null && tpe != NoType

    /** Is `name` a legal identifier (i.e. consisting only of word `\w+` characters)? */
    def encoded(name: Name): Boolean =
      name == name.encodedName

    /** Is `sym` a legal identifier (i.e. having an `encoded` name)? */
    def encoded(sym: Symbol): Boolean =
      is.encoded(sym.name)

    /** Is `sym` local (its owner being a binding or a method)? */
    def local(sym: Symbol): Boolean = {
      val owner = sym.owner
      !owner.isPackage && !owner.isClass && !owner.isModule
    }

    /** Is `sym` overloaded (i.e. having variants with different type signatures). */
    def overloaded(sym: Symbol): Boolean =
      sym.isTerm && sym.asTerm.isOverloaded

    /** Is `sym` the `_root_` symbol? */
    def root(sym: Symbol): Boolean =
      sym == rootMirror.RootPackage || sym == rootMirror.RootClass

    /** Is `sym` a term symbol? */
    def term(sym: Symbol): Boolean =
      sym.isTerm

    /** Is `sym` a type symbol? */
    def tpe(sym: Symbol): Boolean =
      sym.isType

    /** Is `sym` a binding symbol (i.e. value, variable or parameter)? */
    def binding(sym: Symbol): Boolean = is.term(sym) && {
      val term = sym.asTerm
      term.isVal || term.isVar || term.isParameter
    }

    /** Is `sym` a (method / lambda / class) parameter symbol? */
    def param(sym: Symbol): Boolean =
      sym.isParameter

    /** Is `sym` a value (`val`) symbol? */
    def value(sym: Symbol): Boolean =
      sym.isTerm && !sym.isParameter && sym.asTerm.isVal

    /** Is `sym` a variable (`var`) symbol? */
    def variable(sym: Symbol): Boolean =
      sym.isTerm && sym.asTerm.isVar

    /** Is `sym` a `class` symbol? */
    def clazz(sym: Symbol): Boolean =
      sym.isClass

    /** Is `sym` a method (`def`) symbol? */
    def method(sym: Symbol): Boolean =
      sym.isMethod

    /** Is `sym` a module (`object`) symbol? */
    def module(sym: Symbol): Boolean =
      sym.isModule

    /** Is `sym` a `package` symbol? */
    def pkg(sym: Symbol): Boolean =
      sym.isPackage

    /** Is `tpe` legal for a term (i.e. not of a higher kind or method)? */
    def result(tpe: Type): Boolean =
      !is.poly(tpe) && !is.method(tpe)

    /** Is `tpe` the type of a method (illegal for a term)? */
    @tailrec
    def method(tpe: Type): Boolean = tpe match {
      case PolyType(_, result) => is.method(result)
      case _: NullaryMethodType => true
      case _: MethodType => true
      case _ => false
    }

    /** Is `tpe` of a higher kind (taking type arguments)? */
    def poly(tpe: Type): Boolean =
      tpe.takesTypeArgs

    /** Does `tree` define a symbol that owns the children of `tree`? */
    def owner(tree: Tree): Boolean = tree match {
      case _: Bind => false
      case _: Function => true
      case _: LabelDef => false
      case _ => tree.isDef
    }

    /** Is `tree` a statement? */
    def stat(tree: Tree): Boolean = tree match {
      case _: Assign => true
      case _: Bind => false
      case _: LabelDef => true
      case _ => tree.isDef
    }

    /** Is `tree` a term? */
    def term(tree: Tree): Boolean = tree match {
      case Ident(termNames.WILDCARD) => false
      case (_: Ident) withSym sym withType tpe => is.term(sym) && is.result(tpe)
      case (_: Select) withSym sym withType tpe => is.term(sym) && is.result(tpe)
      case (_: Apply) withType tpe => is.result(tpe)
      case (_: TypeApply) withType tpe => is.result(tpe)
      case _: Assign => false
      case _: Bind => false
      case _: LabelDef => false
      case _: New => false
      case _ => tree.isTerm
    }

    /** Is `tree` a quoted type-tree? */
    def tpe(tree: Tree): Boolean = tree match {
      case (_: Ident) withSym sym => is.tpe(sym)
      case (_: Select) withSym sym => is.tpe(sym)
      case _: New => true
      case _ => tree.isType
    }

    /** Is `tree` a valid pattern? */
    def pattern(tree: Tree): Boolean = tree match {
      case Ident(termNames.WILDCARD) => true
      case (_: Ident) withSym sym withType tpe => is.term(sym) && is.result(tpe)
      case (_: Select) withSym sym withType tpe => is.tpe(sym) && is.result(tpe)
      case (_: Apply) withType tpe => is.result(tpe)
      case _: Alternative => true
      case _: Bind => true
      case _: Typed => true
      case _ => false
    }
  }

  object are {

    /** Are `flags` a superset of `property`? */
    def apply(property: FlagSet)(flags: FlagSet): Boolean =
      (flags | property) == flags

    /** The opposite of `are(property)(flags)`. */
    def not(property: FlagSet)(flags: FlagSet): Boolean =
      (flags | property) != flags

    /** Are all `trees` non-degenerate? */
    def defined(trees: Traversable[Tree]): Boolean =
      trees.forall(is.defined)

    /** Are all `trees` statements? */
    def stats(trees: Traversable[Tree]): Boolean =
      trees.forall(is.stat)

    /** Are all `trees` terms? */
    def terms(trees: Traversable[Tree]): Boolean =
      trees.forall(is.term)

    /** Are all `trees` quoted type-trees? */
    def types(trees: Traversable[Tree]): Boolean =
      trees.forall(is.tpe)
  }

  object has {

    /** Does `sym` have a name? */
    def name(sym: Symbol): Boolean =
      is.defined(sym.name)

    /** Does `sym` have an owner? */
    def owner(sym: Symbol): Boolean =
      is.defined(sym.owner)

    /** Does `tree` have an owner? */
    def owner(tree: Tree): Boolean =
      has.owner(tree.symbol)

    /** Does `tpe` have an owner? */
    def owner(tpe: Type): Boolean = has.owner {
      if (has.typeSym(tpe)) tpe.typeSymbol
      else tpe.termSymbol
    }

    /** Does `sym` have a position? */
    def pos(sym: Symbol): Boolean =
      is.defined(sym.pos)

    /** Does `tree` have a position? */
    def pos(tree: Tree): Boolean =
      is.defined(tree.pos)

    /** Does `tree` reference a symbol? */
    def sym(tree: Tree): Boolean =
      is.defined(tree.symbol)

    /** Does `tpe` reference a symbol? */
    def sym(tpe: Type): Boolean =
      has.termSym(tpe) || has.typeSym(tpe)

    /** Does `tree` reference a term symbol? */
    def termSym(tree: Tree): Boolean =
      has.sym(tree) && tree.symbol.isTerm

    /** Does `tpe` reference a term symbol? */
    def termSym(tpe: Type): Boolean =
      is.defined(tpe.termSymbol)

    /** Does `tree` reference a type symbol? */
    def typeSym(tree: Tree): Boolean =
      has.sym(tree) && tree.symbol.isType

    /** Does `tpe` reference a type symbol? */
    def typeSym(tpe: Type): Boolean =
      is.defined(tpe.typeSymbol)

    /** Does `tree` have a type? */
    def tpe(tree: Tree): Boolean =
      is.defined(tree.tpe)

    /** Does `sym` have a type? */
    def tpe(sym: Symbol): Boolean =
      is.defined(sym.info)
  }

  object have {

    /** Do all `symbols` have a name? */
    def name(symbols: Traversable[Symbol]): Boolean =
      symbols.forall(has.name)

    /** Do all `symbols` have an owner? */
    def owner(symbols: Traversable[Symbol]): Boolean =
      symbols.forall(has.owner)

    /** Do all `trees` have a position? */
    def pos(trees: Traversable[Tree]): Boolean =
      trees.forall(has.pos)

    /** Do all `trees` have a symbol? */
    def sym(trees: Traversable[Tree]): Boolean =
      trees.forall(has.sym)

    /** Do all `trees` have a term symbol? */
    def termSym(trees: Traversable[Tree]): Boolean =
      trees.forall(has.termSym)

    /** Do all `trees` have a type symbol? */
    def typeSym(trees: Traversable[Tree]): Boolean =
      trees.forall(has.typeSym)

    /** Do all `trees` have a type? */
    def tpe(trees: Traversable[Tree]): Boolean =
      trees.forall(has.tpe)
  }

  // ------
  // Flags
  // ------

  /** All explicit flags. */
  lazy val Flags = Set(
    Flag.ABSOVERRIDE,
    Flag.ABSTRACT,
    Flag.ARTIFACT,
    Flag.BYNAMEPARAM,
    Flag.CASE,
    Flag.CASEACCESSOR,
    Flag.ARTIFACT,
    Flag.CONTRAVARIANT,
    Flag.COVARIANT,
    Flag.DEFAULTINIT,
    Flag.DEFAULTPARAM,
    Flag.DEFERRED,
    Flag.ENUM,
    Flag.FINAL,
    Flag.IMPLICIT,
    Flag.INTERFACE,
    Flag.LAZY,
    Flag.LOCAL,
    Flag.MACRO,
    Flag.MUTABLE,
    Flag.OVERRIDE,
    Flag.PARAM,
    Flag.PARAMACCESSOR,
    Flag.PRIVATE,
    Flag.PROTECTED,
    Flag.SEALED,
    Flag.STABLE,
    Flag.SYNTHETIC,
    Flag.TRAIT)

  // ---------------
  // Virtual nodes
  // ---------------

  /** Common parent for all virtual AST nodes. */
  trait Node {
    override def toString: String =
      getClass.getSimpleName.dropRight(1)
  }
}
