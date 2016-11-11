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

        def check(zipped: Seq[(u.Symbol, u.Tree)]) = zipped
          .forall { case (param, arg) => arg.tpe <:< Type.signature(param) }

        val matching = for {
          alternative <- sym.alternatives
          signature = Type.signature(alternative, in = target)
          if signature.typeParams.size == targs.size
          paramss = Type(signature, targs: _*).paramLists
          (np, na) = (paramss.size, argss.size)
          // This allows to skip implicit parameters.
          if np == na || (np == na + 1 && paramss.last.forall(is(IMPLICIT)))
          if paramss zip argss forall {
            // This allows to handle variable length arguments.
            case (prefix :+ (_ withInfo VarArgType(tpe)), args) =>
              check(prefix zip args) && (args drop prefix.length match {
                case Seq(arg) if arg.tpe <:< Type.kind1[Seq](tpe) => true
                case suffix => suffix.forall(_.tpe <:< tpe)
              })
            case (params, args) =>
              params.size == args.size && check(params zip args)
          }
        } yield alternative

        assert(matching.nonEmpty,  s"Cannot find variant of $sym with matching type signature")
        assert(matching.size == 1, s"Ambiguous resolution of overloaded symbol $sym")
        matching.head
      } else sym

      /**
       * Performs a symbol substitution, given a set of `aliases` and a new owner.
       * @param at The new owner of the tree.
       * @param aliases Pairs of symbols to substitute from -> to.
       * @param in The tree to perform substitution in.
       * @return A structurally equivalent tree, owned by `at`, with all `aliases` substituted.
       */
      def subst(at: u.Symbol, aliases: (u.Symbol, u.Symbol)*)(in: u.Tree): u.Tree = {
        if (!is.defined(at) && aliases.isEmpty) return in
        // NOTE: This mutable state could be avoided if Transducers had attribute initializers.
        var (from, to) = aliases.toList.unzip
        var symAlias = aliases.toMap.withDefault(identity)
        // Handle method types as well.
        def tpeAlias(tpe: u.Type): u.Type =
          if (is.defined(tpe)) {
            if (tpe.typeParams.exists(symAlias.contains) ||
                tpe.paramLists.iterator.flatten.exists(symAlias.contains)) {
              val tps = tpe.typeParams.map(symAlias(_).asType)
              val pss = tpe.paramLists.map(_.map(symAlias(_).asTerm))
              val Res = tpe.finalResultType.substituteSymbols(from, to)
              Type.method(tps: _*)(pss: _*)(Res)
            } else tpe.substituteSymbols(from, to)
          } else tpe

        TopDown.inherit {
          case Owner(sym) => sym
        } (Monoids.right(at)).traverseWith {
          case Attr.inh(Owner(sym), currOwner :: _) =>
            val alias = symAlias(sym)
            val owner = symAlias(currOwner)
            val tpe = alias.info.substituteSymbols(from, to)
            val changed = owner != alias.owner || tpe != alias.info
            if (is.method(alias)) {
              val met = alias.asMethod
              if (changed || met.typeParams.exists(symAlias.contains) ||
                  met.paramLists.iterator.flatten.exists(symAlias.contains)) {
                val tps = met.typeParams.map(symAlias(_).asType)
                val pss = met.paramLists.map(_.map(symAlias(_).asTerm))
                val nme = alias.name.toTermName
                val flg = get.flags(alias)
                val pos = alias.pos
                val Res = tpe.finalResultType
                val dup = DefSym(owner, nme, flg, pos)(tps: _*)(pss: _*)(Res)
                val src = sym :: met.typeParams ::: met.paramLists.flatten
                val dst = dup :: dup.typeParams ::: dup.paramLists.flatten
                symAlias ++= src.iterator zip dst.iterator
                from :::= src
                to :::= dst
              }
            } else if (changed) {
              val dup = copy(alias)(owner = owner, tpe = tpe)
              symAlias += sym -> dup
              from ::= sym
              to ::= dup
            }
        } (in)

        // Can't be fused with the traversal above,
        // because method calls might appear before their definition.
        if (symAlias.isEmpty) in
        else TopDown.transform { case tree
          if has.tpe(tree) || (has.sym(tree) && symAlias.contains(tree.symbol))
          => Tree.copy(tree)(sym = symAlias(tree.symbol), tpe = tpeAlias(tree.tpe))
        } (in).tree
      }

      def unapply(sym: u.Symbol): Option[u.Symbol] =
        Option(sym).filter(is.defined)
    }

    /** Named entities that own their children. */
    object Owner extends Node {

      /** Extracts the owner of `sym`, if any. */
      def of(sym: u.Symbol): u.Symbol = {
        assert(is.defined(sym), s"Undefined symbol `$sym` has no $this")
        assert(has.owner(sym), s"Symbol `$sym` has no $this")
        sym.owner
      }

      /** Extracts the owner of `tree`, if any. */
      def of(tree: u.Tree): u.Symbol = tree
        .find(is.owner)
        .map(_.symbol.owner)
        .getOrElse(u.NoSymbol)

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
        Sym.subst(owner)
      }

      def unapply(tree: u.Tree): Option[u.Symbol] = for {
        tree <- Option(tree)
        if is.owner(tree) && has.sym(tree)
      } yield tree.symbol
    }
  }
}
