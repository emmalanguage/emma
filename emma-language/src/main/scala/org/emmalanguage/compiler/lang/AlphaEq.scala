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
package compiler.lang

import compiler.Common

import org.scalactic.Bad
import org.scalactic.Good
import org.scalactic.Or

import scala.collection.mutable

/** Provides alpha equivalence for Scala ASTs. */
trait AlphaEq extends Common {

  import u._
  import internal._

  // ---------------------------------------------------------------------------
  // Types
  // ---------------------------------------------------------------------------

  /** Alpha equivalence. */
  type Eq = Unit

  /** Alpha difference at subtrees `lhs` and `rhs` due to `msg`. */
  case class Neq(lhs: Tree, rhs: Tree, msg: String)

  // ---------------------------------------------------------------------------
  // Type constructors
  // ---------------------------------------------------------------------------

  /** Unconditional success. */
  def pass: Eq Or Neq = Good(())

  /** Unconditional failure. */
  object fail {

    /** Provide differing subtrees. */
    case class at(lhs: Tree, rhs: Tree) {

      /** Provide a reason message. */
      def because(msg: String): Eq Or Neq =
        Bad(Neq(lhs, rhs, msg))
    }
  }

  // ------------------------------------------------------------------------
  // Alpha equivalence function
  // ------------------------------------------------------------------------

  /** Are `lhs` and `rhs` alpha equivalent (i.e. equal upto renaming)? */
  def alphaEq(lhs: Tree, rhs: Tree): Eq Or Neq = {

    // An accumulator dictionary for alpha equivalence.
    var keys = freeTerms(lhs) ::: freeTypes(lhs)
    var vals = freeTerms(rhs) ::: freeTypes(rhs)
    val dict = mutable.Map(keys zip vals: _*)

    /* Updates the dictionary with a alias. */
    def alias(original: Symbol, alias: Symbol): Unit = {
      dict += original -> alias
      keys ::= original
      vals ::= alias
    }

    // ------------------------------------------------------------------------
    // Helper functions
    // ------------------------------------------------------------------------

    // Alpha equivalence of trees
    def trees(lhs: Tree, rhs: Tree): Eq Or Neq = {

      // Provides `failHere` as alias for `fail at (lhs, rhs)`
      lazy val failHere = fail at (lhs, rhs)

      // Alpha equivalence of symbols
      def symbols(lhSym: Symbol, rhSym: Symbol): Eq Or Neq = (lhSym, rhSym) match {
        case _ if lhSym == rhSym =>
          pass

        case ( // Synthetic ToolBox owner
          api.Sym.With.nme(_, api.TermName.exprOwner),
          api.Sym.With.nme(_, api.TermName.exprOwner)
          ) => pass

        case ( // Synthetic ToolBox owner
          api.Sym.With.nme(_, api.TermName.local),
          api.Sym.With.nme(_, api.TermName.local)
          ) => pass

        case _ => for {
          eq <- if (dict contains lhSym) pass
            else failHere because s"No alpha-mapping for symbol $lhSym available"
          eq <- if (dict(lhSym) == rhSym) pass
            else failHere because s"Symbols $lhSym and $rhSym are not alpha equivalent"
          eq <- symbols(lhSym.owner, rhSym.owner)
        } yield eq
      }

      // Alpha equivalence of names
      def names(lhName: Name, rhName: Name): Eq Or Neq =
        if (lhName == rhName) pass
        else failHere because s"Names $lhName and $rhName differ"

      // Alpha equivalence of types
      def types(lhT: Type, rhT: Type): Eq Or Neq =
        if (lhT.substituteSymbols(keys, vals).widen =:= rhT.widen) pass
        else failHere because s"Types $lhT and $rhT differ"

      // Alpha equivalence of constants
      def constants(lhs: Constant, rhs: Constant): Eq Or Neq =
        if (lhs.value == rhs.value) pass
        else failHere because s"Literals ${lhs.value} and ${rhs.value} differ"

      // Alpha equivalence of modifiers
      def modifiers(lhMods: Modifiers, rhMods: Modifiers): Eq Or Neq = {
        if (FlagsNoSynthetic.forall(f => lhMods.hasFlag(f) == rhMods.hasFlag(f))) pass
        else failHere because s"$lhMods and $rhMods differ"
      }

      // Alpha equivalence of sequences of trees
      def all(lhTrees: Seq[Tree], rhTrees: Seq[Tree]): Eq Or Neq =
        (lhTrees zip rhTrees).foldLeft(pass) { case (result, (l, r)) =>
          if (result.isBad) result else trees(l, r)
        }

      // Alpha equivalence of the size of sequences
      def size(lhTrees: Seq[Tree], rhTrees: Seq[Tree], children: String): Eq Or Neq =
        if (lhTrees.size == rhTrees.size) pass
        else failHere because s"Number of $children does not match"

      // Alpha equivalence of the structure of nested sequences
      def shape(lhTrees: Seq[Seq[Tree]], rhTrees: Seq[Seq[Tree]], children: String): Eq Or Neq = {
        lazy val outerSizeEq = lhTrees.size == rhTrees.size
        lazy val innerSizeEq = lhTrees zip rhTrees forall { case (l, r) => l.size == r.size }
        if (outerSizeEq && innerSizeEq) pass
        else failHere because s"Shape of $children does not match"
      }

      (lhs, rhs) match {
        case _ if lhs == rhs =>
          pass

        case _ if lhs.isEmpty && rhs.isEmpty =>
          pass

        case ( // Annotated trees
          Annotated(ann$l, arg$l),
          Annotated(ann$r, arg$r)
          ) => for {
            eq <- trees(ann$l, ann$r)
            eq <- trees(arg$l, arg$r)
          } yield eq

        case ( // `this` references
          This(qual$l),
          This(qual$r)
          ) => for {
            eq <- symbols(lhs.symbol, rhs.symbol)
            name$l = if (is.defined(qual$l)) qual$l else lhs.symbol.name
            name$r = if (is.defined(qual$r)) qual$r else rhs.symbol.name
            eq <- names(name$l, name$r)
          } yield eq

        case ( // Type-trees
          TypeTree(),
          TypeTree()
          ) => for {
            eq <- types(lhs.tpe, rhs.tpe)
          } yield eq

        case ( // Literals
          Literal(const$l),
          Literal(const$r)
          ) => for {
            eq <- constants(const$l, const$r)
          } yield eq

        case ( // References
          lhs: Ident,
          rhs: Ident
          ) => for {
            eq <- symbols(lhs.symbol, rhs.symbol)
          } yield eq

        case ( // Type ascriptions
          Typed(expr$l, tpt$l),
          Typed(expr$r, tpt$r)
          ) => for {
            eq <- trees(expr$l, expr$r)
            eq <- types(tpt$l.tpe, tpt$r.tpe)
          } yield eq

        case ( // Selections
          Select(qual$l, member$l),
          Select(qual$r, member$r)
          ) => for {
            eq <- symbols(lhs.symbol, rhs.symbol)
            eq <- trees(qual$l, qual$r)
            eq <- names(member$l, member$r)
          } yield eq

        case ( // Blocks
          Block(stats$l, expr$l),
          Block(stats$r, expr$r)
          ) => for {
            eq <- size(stats$l, stats$r, "block statements")
            _ = for ((l: DefDef, r: DefDef) <- stats$l zip stats$r)
              alias(l.symbol, r.symbol)
            eq <- all(stats$l, stats$r)
            eq <- trees(expr$l, expr$r)
          } yield eq

        case ( // `val` / `var` / parameter definitions
          ValDef(mods$l, _, tpt$l, rhs$l),
          ValDef(mods$r, _, tpt$r, rhs$r)
          ) => for {
            eq <- modifiers(mods$l, mods$r)
            eq <- types(lhs.symbol.info, rhs.symbol.info)
            _ = alias(lhs.symbol, rhs.symbol)
            eq <- trees(rhs$l, rhs$r)
          } yield eq

        case ( // `var` mutations
          Assign(lhs$l, rhs$l),
          Assign(lhs$r, rhs$r)
          ) => for {
            eq <- trees(lhs$l, lhs$r)
            eq <- trees(rhs$l, rhs$r)
          } yield eq

        case ( // Lambda functions
          Function(params$l, body$l),
          Function(params$r, body$r)
          ) => for {
            eq <- size(params$l, params$r, "function parameters")
            _ = alias(lhs.symbol, rhs.symbol)
            eq <- all(params$l, params$r)
            eq <- trees(body$l, body$r)
          } yield eq

        case ( // Type applications
          TypeApply(target$l, targs$l),
          TypeApply(target$r, targs$r)
          ) => for {
            eq <- symbols(lhs.symbol, rhs.symbol)
            eq <- trees(target$l, target$r)
            eq <- size(targs$l, targs$r, "type arguments")
            eq <- all(targs$l, targs$r)
          } yield eq

        case ( // Applications
          Apply(target$l, args$l),
          Apply(target$r, args$r)
          ) => for {
            eq <- symbols(lhs.symbol, rhs.symbol)
            eq <- trees(target$l, target$r)
            eq <- size(args$l, args$r, "application arguments")
            eq <- all(args$l, args$r)
          } yield eq

        case ( // Instantiations
          New(tpt$l),
          New(tpt$r)
          ) => for {
            eq <- types(tpt$l.tpe, tpt$r.tpe)
          } yield eq

        case ( // Branches
          If(cond$l, then$l, else$l),
          If(cond$r, then$r, else$r)
          ) => for {
            eq <- trees(cond$l, cond$r)
            eq <- trees(then$l, then$r)
            eq <- trees(else$l, else$r)
          } yield eq

        case ( // `while` loops
          LabelDef(_, Nil, If(cond$l, Block(body$l :: Nil, _), _)),
          LabelDef(_, Nil, If(cond$r, Block(body$r :: Nil, _), _))
          ) => for {
            eq <- trees(cond$l, cond$r)
            eq <- trees(body$l, body$r)
          } yield eq

        case ( // `do-while` loops
          LabelDef(_, Nil, Block(body$l :: Nil, If(cond$l, _, _))),
          LabelDef(_, Nil, Block(body$r :: Nil, If(cond$r, _, _)))
          ) => for {
            eq <- trees(cond$l, cond$r)
            eq <- trees(body$l, body$r)
          } yield eq

        case ( // Method definitions
          DefDef(mods$l, _, tparams$l, paramss$l, tpt$l, rhs$l),
          DefDef(mods$r, _, tparams$r, paramss$r, tpt$r, rhs$r)
          ) => for {
            eq <- modifiers(mods$l, mods$r)
            eq <- size(tparams$l, tparams$r, "type parameters")
            eq <- all(tparams$l, tparams$r)
            eq <- shape(paramss$l, paramss$r, "method parameters")
            eq <- all(paramss$l.flatten, paramss$r.flatten)
            eq <- types(tpt$l.tpe, tpt$r.tpe)
            _ = alias(lhs.symbol, rhs.symbol)
            eq <- trees(rhs$l, rhs$r)
          } yield eq

        case ( // Pattern matches
          Match(sel$l, cases$l),
          Match(sel$r, cases$r)
          ) => for {
            eq <- trees(sel$l, sel$r)
            eq <- size(cases$l, cases$r, "pattern cases")
            eq <- all(cases$l, cases$r)
          } yield eq

        case ( // Pattern cases
          CaseDef(pat$l, guard$l, body$l),
          CaseDef(pat$r, guard$r, body$r)
          ) => for {
            eq <- trees(pat$l, pat$r)
            eq <- trees(guard$l, guard$r)
            eq <- trees(body$l, body$r)
          } yield eq

        case ( // Pattern bindings
          Bind(_, pat$l),
          Bind(_, pat$r)
          ) => for {
            eq <- trees(pat$l, pat$r)
            _ = alias(lhs.symbol, rhs.symbol)
          } yield eq

        case _ =>
          failHere because "Unexpected pair of trees"
      }
    }

    for {
      eq <- if (keys.size == vals.size) pass
        else fail at (lhs, rhs) because "Number of free symbols does not match"
      eq <- (keys zip vals).foldLeft(pass) { case (result, (l, r)) =>
        if (result.isBad) result
        else if (l.name == r.name && l.info =:= r.info) pass
        else fail at (lhs, rhs) because s"Free symbols $l and $r don't match"
      }
      eq <- trees(lhs, rhs)
    } yield eq
  }
}
