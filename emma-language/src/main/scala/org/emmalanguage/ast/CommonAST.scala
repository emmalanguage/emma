/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package ast

import scala.annotation.tailrec
import scala.reflect.api.Universe

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros and runtime reflection APIs, e.g. non-idempotent type checking, lack of hygiene,
 * capture-avoiding substitution, fully-qualified names, fresh name generation, identifying
 * closures, etc.
 *
 * This trait has to be instantiated with a [[scala.reflect.api.Universe]] type and works for both
 * runtime and compile time reflection.
 */
trait CommonAST {

  val universe: Universe
  lazy val u: universe.type = universe

  /** Syntax sugar for partial functions. */
  type =?>[-A, +B] = PartialFunction[A, B]

  import universe._
  import definitions._
  import internal._
  import Flag._

  // ----------------------------------------------------------------------------------------------
  // Abstract methods
  // ----------------------------------------------------------------------------------------------

  private[ast] def freshNameSuffix: Char

  /** Returns `tpt` with its original field set. */
  private[ast] def setOriginal(tpt: TypeTree, original: Tree): TypeTree

  /** Returns the enclosing named entity (class, method, value, etc). */
  def enclosingOwner: Symbol

  /** Infers an implicit value from the enclosing context (if possible). */
  def inferImplicit(tpe: Type): Option[Tree]

  /** Raises a compiler warning. */
  def warning(msg: String, pos: Position = NoPosition): Unit

  /** Raises an error and terminates compilation. */
  def abort(msg: String, pos: Position = NoPosition): Nothing

  /** Parses a snippet of source code and returns the AST. */
  def parse(code: String): Tree

  /** Type-checks a `tree` (use `typeMode=true` for type-trees). */
  def typeCheck(tree: Tree, typeMode: Boolean = false): Tree

  /** Removes all type and symbol attributes from a `tree`. */
  // FIXME: Replace with `c.untypecheck` or `tb.untypecheck` once SI-5464 is resolved.
  def unTypeCheck(tree: Tree): Tree =
    parse(showCode(tree, printRootPkg = true))

  /**
   * Evaluates a snippet of code and returns a value of type `T`.
   * Note: this can be called on type--checked trees (as opposed to the eval method in ToolBox).
   */
  def eval[T](code: Tree): T

  // ----------------------------------------------------------------------------------------------
  // Property checks
  // ----------------------------------------------------------------------------------------------

  /** Constant limits. */
  object Max {

    /** Maximum number of lambda arguments. */
    val FunParams = FunctionClass.seq.size

    /** Maximum number of tuple elements. */
    val TupleElems = TupleClass.seq.size
  }

  object is {

    /** Does `sym` have the property encoded as flag(s)? */
    def apply(property: FlagSet)(sym: Symbol): Boolean =
      are(property)(flags(sym))

    /** The opposite of `is(property)(sym)`. */
    def not(property: FlagSet)(sym: Symbol): Boolean =
      are.not(property)(flags(sym))

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

    /** Is `tpe` the type of a case class? */
    def caseClass(tpe: u.Type): Boolean =
      tpe.typeSymbol match {
        case cls: u.ClassSymbol => cls.isCaseClass
        case _ => false
      }

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
      // This is tricky for Java classes
      sym.name == termNames.ROOTPKG || sym.name == rootMirror.RootClass.name

    /** Is `sym` a binding symbol (i.e. value, variable or parameter)? */
    def binding(sym: Symbol): Boolean = sym.isTerm && {
      val term = sym.asTerm
      term.isVal || term.isVar || term.isParameter
    }

    /** Is `sym` a value (`val`) symbol? */
    def value(sym: Symbol): Boolean =
      sym.isTerm && !sym.isParameter && sym.asTerm.isVal

    /** Is `sym` a variable (`var`) symbol? */
    def variable(sym: Symbol): Boolean =
      sym.isTerm && sym.asTerm.isVar

    /** Is `sym` a label (loop) symbol? */
    def label(sym: Symbol): Boolean =
      sym.isMethod && is(CONTRAVARIANT)(sym)

    /** Is `sym` a method (`def`) symbol? */
    def method(sym: Symbol): Boolean =
      sym.isMethod && is.not(CONTRAVARIANT)(sym)

    /** Is `sym` a by-name parameter? */
    def byName(sym: Symbol): Boolean =
      sym.isTerm && sym.asTerm.isByNameParam

    /** Is `sym` a stable identifier? */
    def stable(sym: Symbol): Boolean =
      sym.isTerm && sym.asTerm.isStable

    /** Is `tpe` legal for a term (i.e. not of a higher kind or method)? */
    def result(tpe: Type): Boolean =
      !tpe.takesTypeArgs && !is.method(tpe)

    /** Is `tpe` the type of a method (illegal for a term)? */
    def method(tpe: Type): Boolean =
      !(tpe =:= tpe.finalResultType)

    /** Is `sym` a stable path? */
    @tailrec def stable(tpe: Type): Boolean = tpe match {
      case u.SingleType(u.NoPrefix, _) => true
      case u.SingleType(prefix, _)     => is.stable(prefix)
      case _ => false
    }

    /** Does `tree` define a symbol that owns the children of `tree`? */
    def owner(tree: Tree): Boolean = tree match {
      case _: Bind     => false
      case _: Function => true
      case _: LabelDef => false
      case _ => tree.isDef
    }

    /** Is `tree` a statement? */
    def stat(tree: Tree): Boolean = tree match {
      case _: Assign   => true
      case _: Bind     => false
      case _: LabelDef => true
      case _ => tree.isDef
    }

    /** Is `tree` a term? */
    def term(tree: Tree): Boolean = tree match {
      case Ident(termNames.WILDCARD) => false
      case id: Ident       => id.symbol.isTerm && is.result(id.tpe)
      case sel: Select     => sel.symbol.isTerm && is.result(sel.tpe)
      case app: Apply      => is.result(app.tpe)
      case tapp: TypeApply => is.result(tapp.tpe)
      case _: Assign       => false
      case _: Bind         => false
      case _: LabelDef     => false
      case _: New          => false
      case _ => tree.isTerm
    }

    /** Is `tree` a valid pattern? */
    def pattern(tree: Tree): Boolean = tree match {
      case Ident(termNames.WILDCARD) => true
      case id: Ident =>
        is.stable(id.symbol) && is.result(id.tpe)
      case Apply(target, args) =>
        target.isType && args.nonEmpty && is.result(tree.tpe)
      case _: Alternative => true
      case _: Bind        => true
      case _: Literal     => true
      case _: Typed       => true
      case _: UnApply     => true
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

    /** Are all `trees` valid patterns? */
    def patterns(trees: Traversable[Tree]): Boolean =
      trees.forall(is.pattern)
  }

  object has {

    /** Does `sym` have an owner? */
    def own(sym: Symbol): Boolean =
      is.defined(sym.owner)

    /** Does `sym` have a name? */
    def nme(sym: Symbol): Boolean =
      is.defined(sym.name)

    /** Does `sym` have a type? */
    def tpe(sym: Symbol): Boolean =
      is.defined(sym.info)

    /** Does `sym` have a position? */
    def pos(sym: Symbol): Boolean =
      is.defined(sym.pos)

    /** Does `tree` reference a symbol? */
    def sym(tree: Tree): Boolean =
      is.defined(tree.symbol)

    /** Does `tree` have a type? */
    def tpe(tree: Tree): Boolean =
      is.defined(tree.tpe)

    /** Does `tree` have a position? */
    def pos(tree: Tree): Boolean =
      is.defined(tree.pos)
  }

  object have {

    /** Do all `symbols` have an owner? */
    def own(symbols: Traversable[Symbol]): Boolean =
      symbols.forall(has.own)

    /** Do all `symbols` have a name? */
    def nme(symbols: Traversable[Symbol]): Boolean =
      symbols.forall(has.nme)

    /** Do all `trees` have a symbol? */
    def sym(trees: Traversable[Tree]): Boolean =
      trees.forall(has.sym)

    /** Do all `trees` have a type? */
    def tpe(trees: Traversable[Tree]): Boolean =
      trees.forall(has.tpe)

    /** Do all `trees` have a position? */
    def pos(trees: Traversable[Tree]): Boolean =
      trees.forall(has.pos)
  }

  // ----------------------------------------------------------------------------------------------
  // Flags
  // ----------------------------------------------------------------------------------------------

  lazy val FlagsNoSynthetic =
    Flags - Flag.SYNTHETIC

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

  // ----------------------------------------------------------------------------------------------
  // Virtual nodes
  // ----------------------------------------------------------------------------------------------

  /** Common parent for all virtual AST nodes. */
  trait Node {
    override def toString: String =
      getClass.getSimpleName.dropRight(1)
  }
}
