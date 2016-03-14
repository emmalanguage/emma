package eu.stratosphere.emma
package compiler

import scala.annotation.tailrec
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
trait Util {

  val universe: Universe
  type ~>[A, B] = PartialFunction[A, B]

  import universe._
  import internal.reificationSupport._

  /** Raise a warning. */
  def warning(pos: Position, msg: String): Unit

  /** Raise an error. */
  def abort(pos: Position, msg: String): Nothing

  /** Parses a snippet of source code and returns the AST. */
  def parse(code: String): Tree

  /** Type-checks a [[Tree]] (use `typeMode=true` for [[TypeTree]]s). */
  def typeCheck(tree: Tree, typeMode: Boolean = false): Tree

  /** Returns a new [[TermSymbol]]. */
  def termSymbol(owner: Symbol, name: TermName,
    flags: FlagSet = Flag.SYNTHETIC,
    pos: Position = NoPosition): TermSymbol

  /** Returns a new [[TypeSymbol]]. */
  def typeSymbol(owner: Symbol, name: TypeName,
    flags: FlagSet = Flag.SYNTHETIC,
    pos: Position = NoPosition): TypeSymbol

  /** Returns a fresh [[TermName]] starting with `prefix`. */
  def freshTerm(prefix: String): TermName =
    if (prefix.nonEmpty && prefix.last == '$') freshTermName(prefix)
    else freshTermName(s"$prefix$$")

  /** Returns a fresh [[TypeName]] starting with `prefix`. */
  def freshType(prefix: String): TypeName =
    if (prefix.nonEmpty && prefix.last == '$') freshTypeName(prefix)
    else freshTypeName(s"$prefix$$")

  // FIXME: Replace with `c.untypecheck` once SI-5464 is resolved.
  /** Removes all [[Type]] and [[Symbol]] attributes from a [[Tree]]. */
  def unTypeCheck(tree: Tree): Tree =
    parse(showCode(tree, printRootPkg = true))

  /** Transforms `tree` recursively in a top-down fashion. */
  def preWalk(tree: Tree)(pf: Tree ~> Tree): Tree =
    new Transformer {
      override def transform(tree: Tree): Tree =
        if (pf.isDefinedAt(tree)) pf(tree)
        else super.transform(tree)
    }.transform(tree)

  /** Transforms `tree` recursively in a bottom-up fashion. */
  def postWalk(tree: Tree)(pf: Tree ~> Tree): Tree =
    new Transformer {
      override def transform(tree: Tree): Tree = {
        val result = super.transform(tree)
        if (pf.isDefinedAt(result)) pf(result)
        else result
      }
    }.transform(tree)

  /** Traverses `tree` recursively in a top-down fashion. */
  def topDown[U](tree: Tree)(pf: Tree ~> U): Unit =
    new Traverser {
      @tailrec
      override def traverse(tree: Tree) =
        if (pf.isDefinedAt(tree)) pf(tree)
        else tree match {
          // NOTE: TypeTree.original is not traversed by default
          case tt: TypeTree if tt.original != null =>
            traverse(tt.original)
          case _ =>
            super.traverse(tree)
        }
    }.traverse(tree)

  /** Traverses `tree` recursively in a bottom-up fashion. */
  def bottomUp[U](tree: Tree)(pf: Tree ~> U): Unit =
    new Traverser {
      override def traverse(tree: Tree) = {
        tree match {
          // NOTE: TypeTree.original is not traversed by default
          case tt: TypeTree if tt.original != null =>
            traverse(tt.original)
          case _ =>
            super.traverse(tree)
        }

        if (pf.isDefinedAt(tree)) pf(tree)
      }
    }.traverse(tree)

  /** Utility for testing the existence of [[Tree]] attributes. */
  object Has {

    /** Does `tree` have a [[Type]]? */
    def tpe(tree: Tree): Boolean = {
      val tpe = tree.tpe
      tpe != null && tpe != NoType
    }

    /** Does `sym` have a [[Type]]? */
    def tpe(sym: Symbol): Boolean = {
      val tpe = sym.info
      tpe != null && tpe != NoType
    }

    /** Does `tree` have a [[Symbol]]? */
    def sym(tree: Tree): Boolean = {
      val sym = tree.symbol
      sym != null && sym != NoSymbol
    }

    /** Does `tree` have a [[TermSymbol]]? */
    def term(tree: Tree): Boolean =
      Has.sym(tree) && tree.symbol.isTerm

    /** Does `tree` have a [[TypeSymbol]]? */
    def typeSym(tree: Tree): Boolean =
      Has.sym(tree) && tree.symbol.isType

    /** Does `sym` have an owner? */
    def owner(sym: Symbol): Boolean = {
      val owner = sym.owner
      owner != null && owner != NoSymbol &&
        owner != rootMirror.RootClass &&
        owner != rootMirror.RootPackage
    }

    /** Does `tree` have a [[Position]]? */
    def pos(tree: Tree): Boolean = {
      val pos = tree.pos
      pos != null && pos != NoPosition
    }

    /** Does `sym` have a [[Position]]? */
    def pos(sym: Symbol): Boolean = {
      val pos = sym.pos
      pos != null && pos != NoPosition
    }
  }
}
