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

import shapeless._

import scala.annotation.tailrec

trait Symbols { this: AST =>

  trait SymbolAPI { this: API =>

    import universe._
    import definitions._
    import internal._
    import reificationSupport._
    import u.Flag.IMPLICIT

    object Sym extends Node {

      // ------------------------------------------------------------------------------------------
      // Predefined symbols
      // ------------------------------------------------------------------------------------------

      lazy val none    = u.NoSymbol
      lazy val foreach = Type[TraversableOnce[Nothing]]
        .member(TermName.foreach).asMethod

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
        FunctionClass.seq.view.zipWithIndex.map(_.swap).toMap

      /** A set of all lambda function symbols. */
      lazy val funs: Set[u.Symbol] =
        FunctionClass.seq.toSet

      // ------------------------------------------------------------------------------------------
      // Utility methods
      // ------------------------------------------------------------------------------------------

      /** Returns the flags associated with `sym`. */
      def flags(sym: u.Symbol): u.FlagSet =
        internal.flags(sym)

      /** Returns modifiers corresponding to the definition of `sym`. */
      def mods(sym: u.Symbol): u.Modifiers =
        u.Modifiers(flags(sym), TypeName.empty, sym.annotations.map(_.tree))

      /** Returns any annotation of type `A` associated with `sym`. */
      def findAnn[A: TypeTag](sym: u.Symbol): Option[u.Annotation] =
        sym.annotations.find(_.tree.tpe =:= Type[A])

      /** Copy / extractor for symbol attributes. */
      object With {

        /** Creates a copy of `sym`, optionally changing some of its attributes. */
        def apply[S <: u.Symbol](sym: S)(
          own: u.Symbol          = sym.owner,
          nme: u.Name            = sym.name,
          tpe: u.Type            = sym.info,
          pos: u.Position        = sym.pos,
          flg: u.FlagSet         = flags(sym),
          ans: Seq[u.Annotation] = Seq.empty
        ): S = {
          // Optimize when there are no changes.
          val noChange = own == sym.owner &&
            nme == sym.name &&
            tpe == sym.info &&
            pos == sym.pos &&
            flg == flags(sym) &&
            ans == sym.annotations

          if (noChange) sym else {
            assert(is.defined(sym), "Undefined symbol cannot be copied")
            assert(!sym.isPackage, s"Package symbol $sym cannot be copied")
            assert(is.defined(nme), "Undefined name")
            assert(is.defined(tpe), "Undefined type")
            val dup = if (sym.isType) {
              val typeNme = nme.encodedName.toTypeName
              if (sym.isClass) newClassSymbol(own, typeNme, pos, flg)
              else newTypeSymbol(own, typeNme, pos, flg)
            } else {
              val termNme = nme.encodedName.toTermName
              if (sym.isModule) newModuleAndClassSymbol(own, termNme, pos, flg)._1
              else if (sym.isMethod) newMethodSymbol(own, termNme, pos, flg)
              else newTermSymbol(own, termNme, pos, flg)
            }

            setInfo(dup, tpe.dealias.widen)
            setAnnotations(dup, ans.toList)
            dup.asInstanceOf[S]
          }
        }

        def unapply(sym: u.Symbol)
          : Option[(u.Symbol, (u.Symbol, u.Name, u.Type, u.Position, u.FlagSet, Seq[u.Annotation]))]
          = for (s <- Option(sym) if s != none)
            yield s -> (s.owner, s.name, s.info, s.pos, flags(s), s.annotations)

        object own {
          def apply[S <: u.Symbol](sym: S, own: u.Symbol): S = With(sym)(own = own)
          def unapply(sym: u.Symbol): Option[(u.Symbol, u.Symbol)] =
            for (s <- Option(sym) if s != none && has.own(s)) yield s -> s.owner
        }

        object nme {
          def apply[S <: u.Symbol](sym: S, nme: u.Name): S = With(sym)(nme = nme)
          def unapply(sym: u.Symbol): Option[(u.Symbol, u.Name)] =
            for (s <- Option(sym) if s != none && has.nme(s)) yield s -> s.name
        }

        object tpe {
          def apply[S <: u.Symbol](sym: S, tpe: u.Type): S = With(sym)(tpe = tpe)
          def unapply(sym: u.Symbol): Option[(u.Symbol, u.Type)] =
            for (s <- Option(sym) if s != none && has.tpe(s)) yield s -> s.info
        }

        object pos {
          def apply[S <: u.Symbol](sym: S, pos: u.Position): S = With(sym)(pos = pos)
          def unapply(sym: u.Symbol): Option[(u.Symbol, u.Position)] =
            for (s <- Option(sym) if s != none && has.pos(s)) yield s -> s.pos
        }

        object flg {
          def apply[S <: u.Symbol](sym: S, flg: u.FlagSet): S = With(sym)(flg = flg)
          def unapply(sym: u.Symbol): Option[(u.Symbol, u.FlagSet)] =
            for (s <- Option(sym) if s != none) yield s -> flags(s)
        }

        object ans {
          def apply[S <: u.Symbol](sym: S, ans: Seq[u.Annotation]): S = With(sym)(ans = ans)
          def unapply(sym: u.Symbol): Option[(u.Symbol, Seq[u.Annotation])] =
            for (s <- Option(sym) if s != none) yield s -> s.annotations
        }
      }

      /** Finds a version of an overloaded symbol with matching type signature, if possible. */
      def resolveOverloaded(target: u.Type, sym: u.Symbol,
        targs: Seq[u.Type]      = Seq.empty,
        argss: Seq[Seq[u.Tree]] = Seq.empty
      ): u.Symbol = if (!is.overloaded(sym)) sym else {
        def check(zipped: Seq[(u.Symbol, u.Tree)]) = zipped.forall {
          case (param, arg) => arg.tpe <:< Type.signature(param)
        }

        val matching = for {
          alternative <- sym.alternatives
          signature = Type.signature(alternative, in = target)
          if signature.typeParams.size == targs.size
          paramss = Type(signature, targs).paramLists
          (np, na) = (paramss.size, argss.size)
          // This allows to skip implicit parameters.
          if np == na || (np == na + 1 && paramss.last.forall(is(IMPLICIT, _)))
          if paramss zip argss forall {
            // This allows to handle variable length arguments.
            case (prefix :+ With.tpe(_, VarArgType(tpe)), args) =>
              check(prefix zip args) && (args drop prefix.length match {
                case Seq(arg) if arg.tpe <:< Type.kind1[Seq](tpe) => true
                case suffix => suffix.forall(_.tpe <:< tpe)
              })
            case (params, args) =>
              params.size == args.size && check(params zip args)
          }
        } yield alternative

        def moreSpecificExists(overloaded: u.Symbol, in: Seq[u.Symbol]) = {
          val overSign = Type.signature(overloaded, in = target)
          val xss = Type(overSign, targs).paramLists
          in.filter(_ != overloaded).exists { candidate =>
            val candSign = Type.signature(candidate, in = target)
            val yss = Type(candSign, targs).paramLists
            (xss zip yss) forall { case (xs, ys) =>
              (xs zip ys) forall { case (x, y) =>
                Type.signature(y) <:< Type.signature(x)
              }
            }
          }
        }

        /* Prune matching methods whose signature is more general than another mathing methods. */
        @tailrec def prune(candidates: Seq[u.Symbol]): Seq[u.Symbol] =
          if (candidates.size <= 1) candidates else {
            val pruned = candidates.filterNot(m => moreSpecificExists(m, candidates))
            if (pruned.size == candidates.size) candidates
            else prune(pruned)
          }

        val pruned = prune(matching)
        assert(pruned.nonEmpty,  s"Cannot find variant of $sym with matching type signature")
        assert(pruned.size == 1, s"Ambiguous resolution of overloaded symbol $sym")
        pruned.head
      }

      /**
       * Performs a symbol substitution, given a set of `aliases` and a new owner.
       * @param at The new owner of the tree.
       * @param aliases Pairs of symbols to substitute from -> to.
       * @return A structurally equivalent tree, owned by `at`, with all `aliases` substituted.
       */
      def subst(at: u.Symbol,
        aliases: Seq[(u.Symbol, u.Symbol)] = Seq.empty
      ): u.Tree => u.Tree = tree =>
        if (!is.defined(at) && aliases.isEmpty) tree else {
          // NOTE: This mutable state could be avoided if Transducers had attribute initializers.
          var (keys, vals) = aliases.toList.unzip
          var dict = aliases.toMap.withDefault(identity)
          // Handle method types as well.
          def subst(tpe: u.Type): u.Type =
            if (is.defined(tpe)) {
              if (tpe.typeParams.exists(dict.contains) ||
                tpe.paramLists.iterator.flatten.exists(dict.contains)) {
                val tps = tpe.typeParams.map(dict(_).asType)
                val pss = tpe.paramLists.map(_.map(dict(_).asTerm))
                val res = tpe.finalResultType.substituteSymbols(keys, vals)
                Type.method(tps, pss, res)
              } else tpe.substituteSymbols(keys, vals)
            } else tpe

          TopDown.withOwner(at).traverseWith {
            case Attr.inh(Owner(sym), cur :: _) =>
              val als = dict(sym)
              val own = dict(cur)
              val tpe = als.info.substituteSymbols(keys, vals)
              val changed = own != als.owner || tpe != als.info
              if (is.method(als)) {
                val met = als.asMethod
                if (changed || met.typeParams.exists(dict.contains) ||
                  met.paramLists.iterator.flatten.exists(dict.contains)) {
                  val nme = als.name.toTermName
                  val tps = met.typeParams.map(dict(_).asType)
                  val pss = met.paramLists.map(_.map(dict(_).asTerm))
                  val res = tpe.finalResultType
                  val flg = flags(als)
                  val pos = als.pos
                  val ans = als.annotations
                  val dup = DefSym(own, nme, tps, pss, res, flg, pos, ans)
                  val src = sym :: met.typeParams ::: met.paramLists.flatten
                  val dst = dup :: dup.typeParams ::: dup.paramLists.flatten
                  dict ++= src.iterator zip dst.iterator
                  keys :::= src
                  vals :::= dst
                }
              } else if (changed) {
                val dup = With(als)(own = own, tpe = tpe)
                dict += sym -> dup
                keys ::= sym
                vals ::= dup
              }
          } (tree)

          // Can't be fused with the traversal above,
          // because method calls might appear before their definition.
          if (dict.isEmpty) tree else TopDown.transform { case t
            if has.tpe(t) || (has.sym(t) && dict.contains(t.symbol))
            => Tree.With(t)(sym = dict(t.symbol), tpe = subst(t.tpe))
          } (tree).tree
        }

      def unapply(sym: u.Symbol): Option[u.Symbol] =
        Option(sym).filter(is.defined)
    }

    /** Named entities that own their children. */
    object Owner extends Node {

      /** Fixes the owner chain at the enclosing owner. */
      lazy val atEncl = at(encl)

      /** Returns the enclosing owner in the current compilation context. */
      def encl: u.Symbol = enclosingOwner

      /** Extracts the owner of `tree`, if any. */
      def of(tree: u.Tree): u.Symbol = tree
        .find(is.owner)
        .map(_.symbol.owner)
        .getOrElse(Sym.none)

      /** Returns a chain of the owners of `sym` starting at `sym` and ending at `_root_`. */
      def chain(sym: u.Symbol): Stream[u.Symbol] =
        Stream.iterate(sym)(_.owner).takeWhile(is.defined)

      /** Fixes the owner chain of a tree with `owner` at the root. */
      def at(owner: u.Symbol): u.Tree => u.Tree = {
        assert(is.defined(owner), "Undefined owner")
        Sym.subst(owner)
      }

      def unapply(tree: u.Tree): Option[u.Symbol] = for {
        tree <- Option(tree) if is.owner(tree) && has.sym(tree)
      } yield tree.symbol
    }
  }
}
