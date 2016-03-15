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

  /** Removes all [[Type]] and [[Symbol]] attributes from a [[Tree]]. */
  // FIXME: Replace with `c.untypecheck` once SI-5464 is resolved.
  def unTypeCheck(tree: Tree): Tree =
    parse(showCode(tree, printRootPkg = true))

  /** Shorthand for one-shot `transform(pf).apply(tree)`. */
  def transform(tree: Tree)(pf: Tree ~> Tree): Tree =
    transform(pf).apply(tree)

  /**
   * Returns a function that performs a depth-first top-down transformation of a tree using the
   * provided partial function as a template. Recognizes all Scala trees. Does not descend into the
   * result of `pf`.
   *
   * == Example ==
   *
   * {{{
   *   var counter = 0
   *   transform {
   *     case vd: ValDef => vd
   *     case q"${_: Int}" =>
   *       counter += 1
   *       q"$counter"
   *   }.apply(reify {
   *     val x = 42
   *     val y = x + 313
   *     y - 55
   *   }) // =>
   *
   *   reify {
   *     val x = 42
   *     val y = x + 313
   *     y - 1
   *   }
   * }}}
   *
   * @param pf A [[scala.PartialFunction]] that returns the desired result for matched trees.
   * @return A new [[scala.reflect.api.Trees.Transformer.transform()]] that applies `pf`.
   */
  def transform(pf: Tree ~> Tree): Tree => Tree =
    new Transformer {
      override def transform(tree: Tree): Tree =
        if (pf.isDefinedAt(tree)) pf(tree)
        else super.transform(tree)
    }.transform

  /** Shorthand for one-shot `preWalk(pf).apply(tree)`. */
  def preWalk(tree: Tree)(pf: Tree ~> Tree): Tree =
    preWalk(pf).apply(tree)

  /**
   * Returns a function that performs a depth-first pre-order transformation of a tree using the
   * provided partial function as a template. Recognizes all Scala trees. Descends into the result
   * of `pf`.
   *
   * WARNING: Make sure the transformation terminates!
   *
   * == Example ==
   *
   * {{{
   *   var counter = 0
   *   preWalk {
   *     case vd: ValDef => vd
   *     case q"${_: Int}" =>
   *       counter += 1
   *       q"$counter"
   *   }.apply(reify {
   *     val x = 42
   *     val y = x + 313
   *     y - 55
   *   }) // =>
   *
   *   reify {
   *     val x = 1
   *     val y = x + 2
   *     y - 3
   *   }
   * }}}
   *
   * @param pf A [[scala.PartialFunction]] that returns the desired result for matched trees.
   * @return A new [[scala.reflect.api.Trees.Transformer.transform()]] that applies `pf`.
   */
  def preWalk(pf: Tree ~> Tree): Tree => Tree =
    new Transformer {
      override def transform(tree: Tree): Tree =
        super.transform {
          if (pf.isDefinedAt(tree)) pf(tree)
          else tree
        }
    }.transform

  /** Shorthand for one-shot `postWalk(pf).apply(tree)`. */
  def postWalk(tree: Tree)(pf: Tree ~> Tree): Tree =
    postWalk(pf).apply(tree)

  /**
   * Returns a function that performs a depth-first post-order transformation of a tree using the
   * provided partial function as a template. Recognizes all Scala trees. Arguments to `pf` have
   * already been walked recursively.
   *
   * == Example ==
   *
   * {{{
   *   var counter = 0
   *   postWalk {
   *     case vd: ValDef => vd
   *     case q"${i: Int}" =>
   *       counter += 1
   *       q"$counter + $i"
   *   }.apply(reify {
   *     val x = 42
   *     val y = x + 313
   *     y - 55
   *   }) // =>
   *
   *   reify {
   *     val x = 1 + 42
   *     val y = 4 + ((2 + x) + (3 + 313))
   *     (7 + (5 + y) - (6 + 55))
   *   }
   * }}}
   *
   * @param pf A [[scala.PartialFunction]] that returns the desired result for matched trees.
   * @return A new [[scala.reflect.api.Trees.Transformer.transform()]] that applies `pf`.
   */
  def postWalk(pf: Tree ~> Tree): Tree => Tree =
    new Transformer {
      override def transform(tree: Tree): Tree = {
        val result = super.transform(tree)
        if (pf.isDefinedAt(result)) pf(result) else result
      }
    }.transform

  /** Shorthand for one-shot `traverse(pf).apply(tree)`. */
  def traverse[U](tree: Tree)(pf: Tree ~> U): Unit =
    traverse(pf).apply(tree)

  /**
   * Returns a function that performs a depth-first top-down traversal of a tree using the provided
   * partial function for callback. Recognizes all Scala trees. Does not descend into trees matched
   * by `pf`.
   *
   * @param pf A [[scala.PartialFunction]] that performs the desired effect for matched trees.
   * @return A new [[scala.reflect.api.Trees.Traverser.traverse()]] that calls `pf`.
   */
  def traverse[U](pf: Tree ~> U): Tree => Unit =
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
    }.traverse

  /** Shorthand for one-shot `topDown(pf).apply(tree)`. */
  def topDown[U](tree: Tree)(pf: Tree ~> U): Unit =
    topDown(pf).apply(tree)

  /**
   * Returns a function that performs a depth-first top-down traversal of a tree using the provided
   * partial function for callback. Recognizes all Scala trees. Descends into trees matched by `pf`.
   *
   * @param pf A [[scala.PartialFunction]] that performs the desired effect for matched trees.
   * @return A new [[scala.reflect.api.Trees.Traverser.traverse()]] that calls `pf`.
   */
  def topDown[U](pf: Tree ~> U): Tree => Unit =
    new Traverser {
      @tailrec
      override def traverse(tree: Tree) = {
        if (pf.isDefinedAt(tree)) pf(tree)
        tree match {
          // NOTE: TypeTree.original is not traversed by default
          case tt: TypeTree if tt.original != null =>
            traverse(tt.original)
          case _ =>
            super.traverse(tree)
        }
      }
    }.traverse

  /** Shorthand for one-shot `bottomUp(pf).apply(tree)`. */
  def bottomUp[U](tree: Tree)(pf: Tree ~> U): Unit =
    bottomUp(pf).apply(tree)

  /**
   * Returns a function that performs a depth-first bottom-up traversal of a tree using the provided
   * partial function for callback. Recognizes all Scala trees. Arguments to `pf` have already been
   * traversed recursively.
   *
   * @param pf A [[scala.PartialFunction]] that performs the desired effect for matched trees.
   * @return A new [[scala.reflect.api.Trees.Traverser.traverse()]] that calls `pf`.
   */
  def bottomUp[U](pf: Tree ~> U): Tree => Unit =
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
    }.traverse

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
