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
    import definitions._
    import internal._
    import reificationSupport._
    import u.Flag.IMPLICIT

    object Sym extends Node {

      // Predefined symbols
      lazy val foreach = Type[TraversableOnce[Nothing]]
        .member(TermName.foreach).asMethod

      /** Creates a copy of `sym`, optionally changing some of its attributes. */
      def copy(sym: u.Symbol)(
        name:  u.Name     = sym.name,
        owner: u.Symbol   = sym.owner,
        tpe:   u.Type     = sym.info,
        pos:   u.Position = sym.pos,
        flg: u.FlagSet    = flags(sym)): u.Symbol = {

        // Optimize when there are no changes.
        val noChange = name == sym.name &&
          owner == sym.owner &&
          tpe == sym.info &&
          pos == sym.pos &&
          flg == flags(sym)

        if (noChange) sym else {
          assert(is.defined(sym), s"Undefined symbol `$sym` cannot be copied")
          assert(!sym.isPackage, s"Package symbol `$sym` cannot be copied")
          assert(is.defined(name), s"Undefined name `$name`")
          assert(is.defined(tpe), s"Undefined type `$tpe`")
          setInfo(if (sym.isType) {
            val typeName = name.encodedName.toTypeName
            if (sym.isClass) newClassSymbol(owner, typeName, pos, flg)
            else newTypeSymbol(owner, typeName, pos, flg)
          } else {
            val termName = name.encodedName.toTermName
            if (sym.isModule) newModuleAndClassSymbol(owner, termName, pos, flg)._1
            else if (sym.isMethod) newMethodSymbol(owner, termName, pos, flg)
            else newTermSymbol(owner, termName, pos, flg)
          }, tpe.dealias.widen)
        }
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

      /** Returns the flags associated with `sym`. */
      def flags(sym: u.Symbol): u.FlagSet =
        internal.flags(sym)

      /** Returns any annotation of type `A` associated with `sym`. */
      def findAnn[A: TypeTag](sym: u.Symbol): Option[u.Annotation] =
        sym.annotations.find(_.tree.tpe =:= Type[A])

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

        def moreSpecificExists(m: u.Symbol, in: List[u.Symbol]): Boolean = {
          val mSignature = Type.signature(m, in = target)
          val mParamss = Type(mSignature, targs: _*).paramLists
          in.filterNot(_ == m).exists(x => {
            val xSignature = Type.signature(x, in = target)
            val xParamss = Type(xSignature, targs: _*).paramLists
            (mParamss zip xParamss) forall { case (mParams, xParams) =>
              (mParams zip xParams) forall { case (mParam, xParam) =>
                Type.signature(xParam) <:< Type.signature(mParam)
              }
            }
          })
        }

        /* Prune matching methods whose signature is more general than another mathing methods. */
        @annotation.tailrec
        def prune(inp: List[u.Symbol]): List[u.Symbol] =
          if (inp.size < 1) inp
          else {
            val res = inp.filterNot(m => moreSpecificExists(m, inp))
            if (res.size == inp.size) inp
            else prune(res)
          }

        val pruned = prune(matching)

        assert(pruned.nonEmpty, s"Cannot find variant of `$sym` with matching type signature")
        assert(pruned.size == 1, s"Ambiguous resolution of overloaded symbol `$sym`")
        pruned.head
      } else sym

      /**
       * Performs a symbol substitution, given a set of `aliases` and a new owner.
       * @param at The new owner of the tree.
       * @param aliases Pairs of symbols to substitute from -> to.
       * @param in The tree to perform substitution in.
       * @return A structurally equivalent tree, owned by `at`, with all `aliases` substituted.
       */
      def subst(at: u.Symbol, aliases: (u.Symbol, u.Symbol)*)(in: u.Tree): u.Tree =
        if (!is.defined(at) && aliases.isEmpty) in else {
          // NOTE: This mutable state could be avoided if Transducers had attribute initializers.
          var (keys, vals) = aliases.toList.unzip
          var dict = aliases.toMap.withDefault(identity)
          // Handle method types as well.
          def tpeAlias(tpe: u.Type): u.Type =
            if (is.defined(tpe)) {
              if (tpe.typeParams.exists(dict.contains) ||
                tpe.paramLists.iterator.flatten.exists(dict.contains)) {
                val tps = tpe.typeParams.map(dict(_).asType)
                val pss = tpe.paramLists.map(_.map(dict(_).asTerm))
                val Res = tpe.finalResultType.substituteSymbols(keys, vals)
                Type.method(tps: _*)(pss: _*)(Res)
              } else tpe.substituteSymbols(keys, vals)
            } else tpe

          TopDown.inherit {
            case Owner(sym) => sym
          } (Monoids.right(at)).traverseWith {
            case Attr.inh(Owner(sym), cur :: _) =>
              val als = dict(sym)
              val own = dict(cur)
              val tpe = als.info.substituteSymbols(keys, vals)
              val changed = own != als.owner || tpe != als.info
              if (is.method(als)) {
                val met = als.asMethod
                if (changed || met.typeParams.exists(dict.contains) ||
                  met.paramLists.iterator.flatten.exists(dict.contains)) {
                  val tps = met.typeParams.map(dict(_).asType)
                  val pss = met.paramLists.map(_.map(dict(_).asTerm))
                  val nme = als.name.toTermName
                  val flg = flags(als)
                  val pos = als.pos
                  val res = tpe.finalResultType
                  val dup = DefSym(own, nme, flg, pos)(tps: _*)(pss: _*)(res)
                  val src = sym :: met.typeParams ::: met.paramLists.flatten
                  val dst = dup :: dup.typeParams ::: dup.paramLists.flatten
                  dict ++= src.iterator zip dst.iterator
                  keys :::= src
                  vals :::= dst
                }
              } else if (changed) {
                val dup = copy(als)(owner = own, tpe = tpe)
                dict += sym -> dup
                keys ::= sym
                vals ::= dup
              }
          } (in)

          // Can't be fused with the traversal above,
          // because method calls might appear before their definition.
          if (dict.isEmpty) in else TopDown.transform { case tree
            if has.tpe(tree) || (has.sym(tree) && dict.contains(tree.symbol))
            => Tree.copy(tree)(sym = dict(tree.symbol), tpe = tpeAlias(tree.tpe))
          } (in).tree
        }

      def unapply(sym: u.Symbol): Option[u.Symbol] =
        Option(sym).filter(is.defined)
    }

    /** Named entities that own their children. */
    object Owner extends Node {

      /** Extracts the owner of `tree`, if any. */
      def of(tree: u.Tree): u.Symbol = tree
        .find(is.owner)
        .map(_.symbol.owner)
        .getOrElse(u.NoSymbol)

      /** Returns a chain of the owners of `sym` starting at `sym` and ending at `_root_`. */
      def chain(sym: u.Symbol): Stream[u.Symbol] =
        Stream.iterate(sym)(_.owner).takeWhile(is.defined)

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
