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

import util.Monoids

import shapeless._

trait Symbols { this: AST =>

  trait SymbolAPI { this: API =>

    import universe._
    import u.definitions._
    import u.internal._
    import u.Flag.IMPLICIT

    object Sym extends Node {

      // Predefined symbols
      lazy val foreach = Type[TraversableOnce[Nothing]].member(TermName.foreach).asMethod

      /** Extracts the symbol of `tree`, if any. */
      def of(tree: u.Tree): u.Symbol = {
        assert(is.defined(tree), s"Undefined tree has no $this: $tree")
        assert(has.sym(tree), s"Tree has no $this:\n${Tree.showSymbols(tree)}")
        tree.symbol
      }

      /** Extracts the symbol of `tpe` (preferring the type symbol), if any. */
      def of(tpe: u.Type): u.Symbol = {
        assert(is.defined(tpe), s"Undefined type `$tpe` has no $this")
        assert(has.sym(tpe), s"Type `$tpe` has no $this")
        if (has.typeSym(tpe)) tpe.typeSymbol else tpe.termSymbol
      }

      /** Creates a copy of `sym`, optionally changing some of its attributes. */
      def copy(sym: u.Symbol)(
          name:  u.Name     = sym.name,
          owner: u.Symbol   = sym.owner,
          tpe:   u.Type     = sym.info,
          pos:   u.Position = sym.pos,
          flags: u.FlagSet  = get.flags(sym)): u.Symbol = {

        // Optimize when there are no changes.
        if (name == sym.name && owner == sym.owner && tpe == sym.info &&
          pos == sym.pos && flags == get.flags(sym)) return sym

        assert(is.defined(sym), s"Undefined symbol `$sym` cannot be copied")
        assert(!is.pkg(sym), s"Package symbol `$sym` cannot be copied")
        assert(is.defined(name), s"Undefined name `$name`")
        assert(is.defined(tpe), s"Undefined type `$tpe`")

        val encoded = name.encodedName
        val dup = if (sym.isType) {
          val typeName = encoded.toTypeName
          if (sym.isClass) newClassSymbol(owner, typeName, pos, flags)
          else newTypeSymbol(owner, typeName, pos, flags)
        } else {
          val termName = encoded.toTermName
          if (sym.isModule) newModuleAndClassSymbol(owner, termName, pos, flags)._1
          else if (sym.isMethod) newMethodSymbol(owner, termName, pos, flags)
          else newTermSymbol(owner, termName, pos, flags)
        }

        set.tpe(dup, Type.fix(tpe))
        dup
      }

      /** A map of all tuple symbols by number of elements. */
      lazy val tuple: Map[Int, u.ClassSymbol] =
        TupleClass.seq.view.zipWithIndex.map {
          case (cls, n) => n + 1 -> cls
        }.toMap

      /** A set of all tuple symbols. */
      lazy val tuples: Set[u.Symbol] =
        TupleClass.seq.toSet

      /** A map of all lambda function symbols by number of arguments. */
      lazy val fun: Map[Int, u.ClassSymbol] =
        FunctionClass.seq.view.zipWithIndex
          .map(_.swap).toMap

      /** A set of all lambda function symbols. */
      lazy val funs: Set[u.Symbol] =
        FunctionClass.seq.toSet

      /** Finds a version of an overloaded symbol with matching type signature, if possible. */
      def resolveOverloaded(target: u.Type = u.NoType)
        (sym: u.Symbol, targs: u.Type*)
        (argss: Seq[u.Tree]*): u.Symbol = if (is.overloaded(sym)) {

        val matching = for {
          alt <- sym.alternatives
          signature = Type.signature(alt, in = target)
          if signature.typeParams.size == targs.size
          paramss = Type(signature, targs: _*).paramLists
          (np, na) = (paramss.size, argss.size)
          // NOTE: This allows to skip implicit parameters.
          if np == na || (np == na + 1 && paramss.last.forall(is(IMPLICIT)))
          if paramss.zip(argss).forall {
            case (Seq(_ withInfo VarArgType(tpe)), args) =>
              args.map(Type.of).forall(_ weak_<:< tpe)
            case (params, args) => params.size == args.size &&
              params.zip(args).forall { case (param, arg) =>
                Type.of(arg) weak_<:< Type.signature(param)
              }
          }
        } yield alt

        assert(matching.nonEmpty, s"Cannot find variant of `$sym` with matching type signature")
        assert(matching.size == 1, s"Ambiguous resolution of overloaded symbol `$sym`")
        matching.head
      } else sym

      def unapply(sym: u.Symbol): Option[u.Symbol] =
        Option(sym).filter(is.defined)
    }

    /** Named entities that own their children. */
    object Owner extends Node {

      import Monoids._

      /** Extracts the owner of `sym`, if any. */
      def of(sym: u.Symbol): u.Symbol = {
        assert(is.defined(sym), s"Undefined symbol `$sym` has no $this")
        assert(has.owner(sym), s"Symbol `$sym` has no $this")
        sym.owner
      }

      /** Extracts the owner of `tree`, if any. */
      def of(tree: u.Tree): u.Symbol =
        Owner.of(Sym.of(tree))

      /** Extracts the owner of `tpe`, if any. */
      def of(tpe: u.Type): u.Symbol =
        Owner.of(Sym.of(tpe))

      /** Returns a chain of the owners of `sym` starting at `sym` and ending at `_root_`. */
      def chain(sym: u.Symbol): Stream[u.Symbol] =
        Stream.iterate(sym)(_.owner).takeWhile(is.defined)

      /** Returns a chain of the owners of `tree` starting at `tree` and ending at `_root_`. */
      def chain(tree: u.Tree): Stream[u.Symbol] =
        chain(tree.symbol)

      /** Returns a chain of the owners of `tpe` starting at `tpe` and ending at `_root_`. */
      def chain(tpe: u.Type): Stream[u.Symbol] =
        if (has.sym(tpe)) chain(Sym.of(tpe))
        else Stream.empty

      /** Fixes the owner chain of a tree with `owner` at the root. */
      def at(owner: u.Symbol): u.Tree => u.Tree = {
        assert(is.defined(owner), s"Undefined owner `$owner`")

        def fix(broken: u.Symbol, owner: u.Symbol, dict: Map[u.Symbol, u.Symbol]) = {
          val (from, to) = dict.toList.unzip
          val tpe = Type.of(broken).substituteSymbols(from, to)
          val fixed = Sym.copy(broken)(owner = owner, tpe = tpe)
          Map(broken -> fixed)
        }

        TopDown.inherit { case Owner(sym) => sym } (Monoids.right(owner))
          .accumulateWith[Map[u.Symbol, u.Symbol]] {
            case Attr(Owner(broken), fixed :: _, current :: _, _)
              if fixed.contains(current) && broken.owner != fixed(current) =>
                fix(broken, fixed(current), fixed)
            case Attr(Owner(broken), fixed :: _, current :: _, _)
              if broken.owner != current =>
                fix(broken, current, fixed)
          }.traverseAny.andThen {
            case Attr.acc(tree, fixed :: _) =>
              if (fixed.isEmpty) tree
              else Tree.renameUnsafe(fixed.toSeq: _*)(tree)
          }
      }

      def unapply(tree: u.Tree): Option[u.Symbol] = for {
        tree <- Option(tree)
        if is.owner(tree) && has.sym(tree)
      } yield tree.symbol
    }
  }
}
