package eu.stratosphere.emma
package compiler

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
  type =?>[-A, +B] = PartialFunction[A, B]

  import universe._

  /** Raises a warning. */
  def warning(pos: Position, msg: String): Unit

  /** Raises an error and terminates compilation. */
  def abort(pos: Position, msg: String): Nothing

  /** Provides the ancestor chain for AST transformation and traversal. */
  trait Ancestors {

    def trace: Boolean

    private var ancestorChain: Seq[Tree] =
      Vector.empty

    /**
     * A chain of all ancestors of the current tree on the path to the root node.
     * `ancestors.head` is the immediate parent, `ancestors.last` is the root.
     * Always empty if `trace` is false.
     */
    def ancestors: Seq[Tree] = ancestorChain

    /** Prepends `tree` to the ancestor chain for the duration of `f` (if `trace` is true). */
    def atParent[A](tree: Tree)(f: => A): A = if (trace) {
      ancestorChain +:= tree
      val result = f
      ancestorChain = ancestors.tail
      result
    } else f
  }

  /** A generic AST transformation scheme guided by a template (partial function). */
  abstract class Transformation(val trace: Boolean)
    extends Transformer with Ancestors with (Tree => Tree) {

    currentOwner = enclosingOwner

    /** Override to provide the transformation logic. */
    def template: Tree =?> Tree

    override def apply(tree: Tree): Tree =
      transform(tree)

    override def transform(tree: Tree): Tree = atParent(tree)(tree match {
      // NOTE: TypeTree.original is not transformed by default
      case tpt: TypeTree if tpt.original != null =>
        val original = transform(tpt.original)
        if (original eq tpt.original) tpt else {
          val copy = treeCopy.TypeTree(tpt)
          setOriginal(copy, original)
          copy
        }

      case _ =>
        super.transform(tree)
    })
  }

  /** A generic AST traversal scheme using a partial function for callback. */
  abstract class Traversal(val trace: Boolean)
    extends Traverser with Ancestors with (Tree => Unit) {

    currentOwner = enclosingOwner

    /** Override to provide the callback logic. */
    def callback: Tree =?> Any

    override def apply(tree: Tree): Unit =
      traverse(tree)

    override def traverse(tree: Tree): Unit = atParent(tree)(tree match {
      // NOTE: TypeTree.original is not traversed by default
      case tpt: TypeTree if tpt.original != null => traverse(tpt.original)
      case _ => super.traverse(tree)
    })
  }

  /**
   * A depth-first top-down transformation scheme using the provided partial function as template.
   * Recognizes all Scala trees. Does not descend into the result of `template`.
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
   * @param trace Set to `true` in case the ancestor chain is needed (default: `false`).
   */
  abstract class transform(trace: Boolean = false) extends Transformation(trace) {
    override final def transform(tree: Tree): Tree =
      template.applyOrElse(tree, super.transform)
  }

  object transform {

    /** See [[eu.stratosphere.emma.compiler.Util.transform]]. */
    def apply(pf: Tree =?> Tree): Transformation =
      new transform { override val template = pf }

    /** Shorthand for one-shot `transform(pf).apply(tree)`. */
    def apply(tree: Tree)(pf: Tree =?> Tree): Tree =
      apply(pf)(tree)
  }

  /**
   * A depth-first pre-order transformation scheme using the provided partial function as template.
   * Recognizes all Scala trees. Descends into the result of `template`.
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
   *   }.transform(reify {
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
   * @param trace Set to `true` in case the ancestor chain is needed (default: `false`).
   */
  abstract class preWalk(trace: Boolean = false) extends Transformation(trace) {
    override final def transform(tree: Tree): Tree =
      super.transform(template.applyOrElse(tree, identity[Tree]))
  }

  object preWalk {

    /** See [[eu.stratosphere.emma.compiler.Util.preWalk]]. */
    def apply(pf: Tree =?> Tree): Transformation =
      new preWalk { override val template = pf }

    /** Shorthand for one-shot `preWalk(pf).transform(tree)`. */
    def apply(tree: Tree)(pf: Tree =?> Tree): Tree =
      apply(pf)(tree)
  }

  /**
   * A depth-first post-order transformation scheme using the provided partial function as template.
   * Recognizes all Scala trees. Arguments to `template` have already been walked recursively.
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
   *   }.transform(reify {
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
   * @param trace Set to `true` in case the ancestor chain is needed (default: `false`).
   */
  abstract class postWalk(trace: Boolean = false) extends Transformation(trace) {
    override final def transform(tree: Tree): Tree =
      template.applyOrElse(super.transform(tree), identity[Tree])
  }

  object postWalk {

    /** See [[eu.stratosphere.emma.compiler.Util.postWalk]]. */
    def apply(pf: Tree =?> Tree): Transformation =
      new postWalk { override val template = pf }

    /** Shorthand for one-shot `postWalk(pf).transform(tree)`. */
    def apply(tree: Tree)(pf: Tree =?> Tree): Tree =
      apply(pf)(tree)
  }

  /**
   * A depth-first top-down traversal scheme using the provided partial function for callback.
   * Recognizes all Scala trees. Does not descend into trees matched by `callback`.
   *
   * @param trace Set to `true` in case the ancestor chain is needed (default: `false`).
   */
  abstract class traverse(trace: Boolean = false) extends Traversal(trace) {
    override final def traverse(tree: Tree): Unit =
      callback.applyOrElse(tree, super.traverse)
  }

  object traverse {

    /** See [[eu.stratosphere.emma.compiler.Util.traverse]]. */
    def apply(pf: Tree =?> Any): Traversal =
      new traverse { override val callback = pf }

    /** Shorthand for one-shot `traverse(pf).apply(tree)`. */
    def apply(tree: Tree)(pf: Tree =?> Any): Unit =
      apply(pf)(tree)
  }

  /**
   * A depth-first top-down traversal scheme using the provided partial function for callback.
   * Recognizes all Scala trees. Descends into trees matched by `callback`.
   *
   * @param trace Set to `true` in case the ancestor chain is needed (default: `false`).
   */
  abstract class topDown(trace: Boolean = false) extends Traversal(trace) {
    override final def traverse(tree: Tree): Unit = {
      callback.applyOrElse(tree, (_: Tree) => ())
      super.traverse(tree)
    }
  }

  object topDown {

    /** See [[eu.stratosphere.emma.compiler.Util.topDown]]. */
    def apply(pf: Tree =?> Any): Traversal =
      new topDown { override val callback = pf }

    /** Shorthand for one-shot `topDown(pf).traverse(tree)`. */
    def apply(tree: Tree)(pf: Tree =?> Any): Unit =
      apply(pf)(tree)
  }

  /**
   * A depth-first bottom-up traversal scheme using the provided partial function for callback.
   * Recognizes all Scala trees. Arguments to `callback` have already been traversed recursively.
   *
   * @param trace Set to `true` in case the ancestor chain is needed (default: `false`).
   */
  abstract class bottomUp(trace: Boolean = false) extends Traversal(trace) {
    override final def traverse(tree: Tree): Unit = {
      super.traverse(tree)
      callback.applyOrElse(tree, (_: Tree) => ())
    }
  }

  object bottomUp {

    /** See [[eu.stratosphere.emma.compiler.Util.bottomUp]]. */
    def apply(pf: Tree =?> Any): Traversal =
      new bottomUp { override val callback = pf }

    /** Shorthand for one-shot `bottomUp(pf).traverse(tree)`. */
    def apply(tree: Tree)(pf: Tree =?> Any): Unit =
      apply(pf)(tree)
  }

  // ------------------------
  // Parsing and typechecking
  // ------------------------

  /** Parses a snippet of source code and returns the AST. */
  private[compiler] def parse(code: String): Tree

  /** Type-checks a [[Tree]] (use `typeMode=true` for [[TypeTree]]s). */
  private[compiler] def typeCheck(tree: Tree, typeMode: Boolean = false): Tree

  /** Removes all [[Type]] and [[Symbol]] attributes from a [[Tree]]. */
  // FIXME: Replace with `c.untypecheck` once SI-5464 is resolved.
  private[compiler] def unTypeCheck(tree: Tree): Tree =
    parse(showCode(tree, printRootPkg = true))

  // ------------------------
  // Abstract wrapper methods
  // ------------------------

  private[compiler] def enclosingOwner: Symbol
  private[compiler] def getFlags(sym: Symbol): FlagSet
  private[compiler] def setFlags(sym: Symbol, flags: FlagSet): Unit
  private[compiler] def resetFlags(sym: Symbol, flags: FlagSet): Unit
  private[compiler] def setName(sym: Symbol, name: Name): Unit
  private[compiler] def setOwner(sym: Symbol, owner: Symbol): Unit
  private[compiler] def setPos(tree: Tree, pos: Position): Unit
  private[compiler] def setOriginal(tt: TypeTree, original: Tree): Unit
  private[compiler] def annotate(sym: Symbol, ans: Annotation*): Unit
  private[compiler] def attachments(sym: Symbol): Attachments
  private[compiler] def attachments(tree: Tree): Attachments

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
}
