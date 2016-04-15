package eu.stratosphere.emma
package compiler

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
trait Util {

  val universe: Universe
  type ~>[A, B] = PartialFunction[A, B]

  import universe._

  /** Raises a warning. */
  def warning(pos: Position, msg: String): Unit

  /** Raises an error and terminates compilation. */
  def abort(pos: Position, msg: String): Nothing

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
        else tree match {
          // NOTE: TypeTree.original is not transformed by default
          case tt: TypeTree if tt.original != null =>
            val original = transform(tt.original)
            if (original eq tt.original) tt else {
              val copy = treeCopy.TypeTree(tt)
              setOriginal(copy, original)
              copy
            }

          case _ =>
            super.transform(tree)
        }
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
      override def transform(tree: Tree): Tree = {
        val result = if (pf.isDefinedAt(tree)) pf(tree) else tree
        result match {
          // NOTE: TypeTree.original is not transformed by default
          case tt: TypeTree if tt.original != null =>
            val original = transform(tt.original)
            if (original eq tt.original) tt else {
              val copy = treeCopy.TypeTree(tt)
              setOriginal(copy, original)
              copy
            }

          case _ =>
            super.transform(result)
        }
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
        val result = tree match {
          // NOTE: TypeTree.original is not transformed by default
          case tt: TypeTree if tt.original != null =>
            val original = transform(tt.original)
            if (original eq tt.original) tt else {
              val copy = treeCopy.TypeTree(tt)
              setOriginal(copy, original)
              copy
            }

          case _ =>
            super.transform(tree)
        }

        if (pf.isDefinedAt(result)) pf(result)
        else result
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

  // ------------------------
  // Abstract wrapper methods
  // ------------------------

  private[compiler] def getFlags(sym: Symbol): FlagSet
  private[compiler] def setFlags(sym: Symbol, flags: FlagSet): Unit
  private[compiler] def setName(sym: Symbol, name: Name): Unit
  private[compiler] def setOwner(sym: Symbol, owner: Symbol): Unit
  private[compiler] def setPos(tree: Tree, pos: Position): Unit
  private[compiler] def setOriginal(tt: TypeTree, original: Tree): Unit
  private[compiler] def annotate(sym: Symbol, ans: Annotation*): Unit
  private[compiler] def attachments(sym: Symbol): Attachments
  private[compiler] def attachments(tree: Tree): Attachments

  /** Parses a snippet of source code and returns the AST. */
  private[compiler] def parse(code: String): Tree

  /** Type-checks a [[Tree]] (use `typeMode=true` for [[TypeTree]]s). */
  private[compiler] def typeCheck(
    tree: Tree,
    typeMode: Boolean = false): Tree

  /** Returns a new [[TermSymbol]]. */
  private[compiler] def termSymbol(
    owner: Symbol,
    name: TermName,
    flags: FlagSet = Flag.SYNTHETIC,
    pos: Position = NoPosition): TermSymbol

  /** Returns a new [[TypeSymbol]]. */
  private[compiler] def typeSymbol(
    owner: Symbol,
    name: TypeName,
    flags: FlagSet = Flag.SYNTHETIC,
    pos: Position = NoPosition): TypeSymbol

  /** Removes all [[Type]] and [[Symbol]] attributes from a [[Tree]]. */
  // FIXME: Replace with `c.untypecheck` once SI-5464 is resolved.
  private[compiler] def unTypeCheck(tree: Tree): Tree =
    parse(showCode(tree, printRootPkg = true))
}
