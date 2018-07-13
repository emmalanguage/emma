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

import cats.instances.all._
import shapeless._

import scala.annotation.tailrec

trait Trees { this: AST =>

  trait TreeAPI { this: API =>

    import u._
    import definitions._
    import internal._
    import reificationSupport._

    /** Copy / extractor for tree attributes. */
    object Tree extends Node {

      // Predefined trees
      lazy val Root  = Id(u.rootMirror.RootPackage)
      lazy val Java  = Sel(Root, JavaLangPackage)
      lazy val Scala = Sel(Root, ScalaPackage)

      object With {

        /** Creates a shallow copy of `tree`, preserving its type and setting new attributes. */
        def apply[T <: Tree](tree: T)(
          sym: u.Symbol   = tree.symbol,
          tpe: u.Type     = tree.tpe,
          pos: u.Position = tree.pos
        ): T = {
          // Optimize when there are no changes.
          val noChange = sym == tree.symbol &&
            tpe == tree.tpe &&
            pos == tree.pos

          if (noChange) tree else {
            val dup = tree match {
              case _ if tree.isEmpty =>
                u.EmptyTree
              case u.Alternative(choices) =>
                u.Alternative(choices)
              case u.Annotated(target, arg) =>
                u.Annotated(target, arg)
              case u.AppliedTypeTree(target, args) =>
                u.AppliedTypeTree(target, args)
              case u.Apply(target, args) =>
                u.Apply(target, args)
              case u.Assign(lhs, rhs) =>
                u.Assign(lhs, rhs)
              case u.AssignOrNamedArg(lhs, rhs) =>
                u.AssignOrNamedArg(lhs, rhs)
              case u.Bind(_, pat) =>
                u.Bind(sym.name, pat)
              case u.Block(stats, expr) =>
                u.Block(stats, expr)
              case u.CaseDef(pat, guard, body) =>
                u.CaseDef(pat, guard, body)
              case u.ClassDef(_, _, tparams, impl) =>
                u.ClassDef(Sym.mods(sym), sym.name.toTypeName, tparams, impl)
              case u.CompoundTypeTree(templ) =>
                u.CompoundTypeTree(templ)
              case u.DefDef(_, _, tparams, paramss, tpt, body) =>
                u.DefDef(Sym.mods(sym), sym.name.toTermName, tparams, paramss, tpt, body)
              case u.ExistentialTypeTree(tpt, where) =>
                u.ExistentialTypeTree(tpt, where)
              case u.Function(params, body) =>
                u.Function(params, body)
              case u.Ident(_) =>
                u.Ident(sym.name)
              case u.If(cond, thn, els) =>
                u.If(cond, thn, els)
              case u.Import(qual, selectors) =>
                u.Import(qual, selectors)
              case u.LabelDef(_, params, body) =>
                u.LabelDef(sym.name.toTermName, params, body)
              case u.Literal(const) =>
                u.Literal(const)
              case u.Match(target, cases) =>
                u.Match(target, cases)
              case u.ModuleDef(_, _, impl) =>
                u.ModuleDef(Sym.mods(sym), sym.name.toTermName, impl)
              case u.New(tpt) =>
                u.New(tpt)
              case u.PackageDef(id, stats) =>
                u.PackageDef(id, stats)
              case u.ReferenceToBoxed(id) =>
                u.ReferenceToBoxed(id)
              case u.RefTree(qual, _) =>
                u.RefTree(qual, sym.name)
              case u.Return(expr) =>
                u.Return(expr)
              case u.Select(target, _) =>
                u.Select(target, sym.name)
              case u.SelectFromTypeTree(target, _) =>
                u.SelectFromTypeTree(target, sym.name.toTypeName)
              case u.SingletonTypeTree(ref) =>
                u.SingletonTypeTree(ref)
              case u.Star(elem) =>
                u.Star(elem)
              case u.Super(qual, _) =>
                u.Super(qual, sym.name.toTypeName)
              case u.Template(parents, self, body) =>
                u.Template(parents, self, body)
              case u.This(_) =>
                u.This(sym.name.toTypeName)
              case u.Throw(ex) =>
                u.Throw(ex)
              case u.Try(block, catches, finalizer) =>
                u.Try(block, catches, finalizer)
              case u.TypeApply(target, targs) =>
                u.TypeApply(target, targs)
              case u.TypeBoundsTree(lo, hi) =>
                u.TypeBoundsTree(lo, hi)
              case u.Typed(expr, tpt) =>
                u.Typed(expr, tpt)
              case u.TypeDef(_, _, tparams, rhs) =>
                u.TypeDef(Sym.mods(sym), sym.name.toTypeName, tparams, rhs)
              case _: u.TypeTree =>
                u.TypeTree()
              case u.UnApply(extr, args) =>
                u.UnApply(extr, args)
              case vd @ u.ValDef(_, _, tpt, rhs) => {
                val ret = u.ValDef(Sym.mods(sym), sym.name.toTermName, tpt, rhs)
                // TODO: This should actually be added to all the above cases...
                meta(vd).all.all.foreach{g => meta(ret).update(g)}
                ret
              }
              case other =>
                other
            }

            if (tpe != dup.tpe) setType(dup, tpe)
            if (pos != dup.pos) atPos(pos)(dup)
            if (sym != dup.symbol) setSymbol(dup, sym)
            dup.asInstanceOf[T]
          }
        }

        def unapply(tree: u.Tree): Option[(u.Tree, (u.Symbol, u.Type, u.Position))] =
          for (t <- Option(tree) if tree.nonEmpty) yield t -> (t.symbol, t.tpe, t.pos)

        object sym {
          def apply[T <: u.Tree](tree: T, sym: u.Symbol): T = With(tree)(sym = sym)
          def unapply(tree: u.Tree): Option[(u.Tree, u.Symbol)] =
            for (t <- Option(tree) if tree.nonEmpty && has.sym(t)) yield t -> t.symbol
        }

        object tpe {
          def apply[T <: u.Tree](tree: T, tpe: u.Type): T = With(tree)(tpe = tpe)
          def unapply(tree: u.Tree): Option[(u.Tree, u.Type)] =
            for (t <- Option(tree) if tree.nonEmpty && has.tpe(t)) yield t -> t.tpe
        }

        object pos {
          def apply[T <: u.Tree](tree: T, pos: u.Position): T = With(tree)(pos = pos)
          def unapply(tree: u.Tree): Option[(u.Tree, u.Position)] =
            for (t <- Option(tree) if tree.nonEmpty && has.pos(t)) yield t -> t.pos
        }
      }

      /** Prints `tree` for debugging (most details). */
      def debug(tree: u.Tree): String =
        u.showCode(tree,
          printIds    = true,
          printOwners = true,
          printTypes  = true)

      /** Prints `tree` in parseable form. */
      def show(tree: u.Tree, cleanup: Boolean = false): String = {
        val result = u.showCode(tree, printRootPkg = true)
        if (!cleanup) result else result
          .replaceAll("(_root_.)?scala.", "")
          .replaceAll("(_root_.)?org.emmalanguage.api.", "")
          .replaceAll("(_root_.)?org.emmalanguage.compiler.ir.ComprehensionSyntax.", "")
          .replaceAll("Tuple\\d{1,2}\\[([a-zA-Z0-9\\,\\,\\ ]+)\\]", "($1)")
          .replaceAll("Tuple\\d{1,2}\\.apply\\[([a-zA-Z0-9\\,\\,\\ ]+)\\]", "")
          .replaceAll("\\.apply\\[([a-zA-Z0-9\\,\\,\\ ]+)\\]", "")
          .replaceAll("\\.([a-zA-Z0-9]+)\\[([a-zA-Z0-9\\,\\,\\ ]+)\\]", ".$1")
          .replaceAll(";\n", "\n")
      }

      /** Prints `tree` including owners as comments. */
      def showOwners(tree: u.Tree): String =
        u.showCode(tree, printOwners = true)

      /** Prints `tree` including symbols as comments. */
      def showSymbols(tree: u.Tree): String =
        u.showCode(tree, printIds = true)

      /** Prints `tree` including types as comments. */
      def showTypes(tree: u.Tree): String =
        u.showCode(tree, printTypes = true)

      /** Returns a set of all term definitions in a `tree`. */
      def defs(tree: u.Tree): Set[u.TermSymbol] =
        tree.collect { case TermDef(lhs) => lhs }.toSet

      /** Returns a set of all term references in a `tree`. */
      def refs(tree: u.Tree): Set[u.TermSymbol] =
        tree.collect { case TermRef(target) => target }.toSet

      /** Returns the closure of `tree` as a set. */
      def closure(tree: u.Tree): Set[u.TermSymbol] =
        refs(tree) diff defs(tree) filterNot (_.isStatic)

      /** Returns a set of all binding definitions in `tree`. */
      def bindings(tree: u.Tree): Set[u.TermSymbol] =
        tree.collect { case BindingDef(lhs, _) => lhs }.toSet

      /** Returns a set of all lambdas in `tree`. */
      def lambdas(tree: u.Tree): Set[u.TermSymbol] =
        tree.collect { case Lambda(fun, _, _) => fun }.toSet

      /** Returns a set of all method (`def`) definitions in `tree`. */
      def methods(tree: u.Tree): Set[u.MethodSymbol] = tree.collect {
        case dfn: u.DefDef => dfn.symbol.asMethod
      }.toSet

      /** Returns a set of all variable (`var`) mutations in `tree`. */
      def mutations(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case VarMut(lhs, _) => lhs
      }.toSet

      /** Returns the subset of `closure(tree)` that is modified within `tree`. */
      def closureMod(tree: u.Tree): Set[u.TermSymbol] =
        closure(tree) & mutations(tree)

      /** Returns a set of all parameter definitions in `tree`. */
      def parameters(tree: u.Tree): Set[u.TermSymbol] =
        tree.collect { case ParDef(lhs, _) => lhs }.toSet

      /** Returns a set of all value (`val`) definitions in `tree`. */
      def values(tree: u.Tree): Set[u.TermSymbol] =
        tree.collect { case ValDef(lhs, _) => lhs }.toSet

      /** Returns a set of all variable (`var`) definitions in `tree`. */
      def variables(tree: u.Tree): Set[u.TermSymbol] =
        tree.collect { case VarDef(lhs, _) => lhs }.toSet

      /** Returns a fully-qualified reference to `target` (must be static). */
      def resolveStatic(target: u.Symbol): u.Tree = {
        assert(is.defined(target), "Cannot resolve undefined target")
        assert(target.isStatic,   s"Cannot resolve dynamic target $target")
        Owner.chain(target).takeWhile(!is.root(_)).foldRight[u.Tree](Root) {
          (member, owner) => Sel(owner, member)
        }
      }

      /** Inlines a sequence of binding definitions in a tree by replacing LHS with RHS. */
      def inline(bindings: Seq[u.ValDef]): u.Tree => u.Tree =
        if (bindings.isEmpty) identity else {
          val dict = bindings.map(bind => bind.symbol -> bind.rhs).toMap
          TopDown.break.transform {
            case BindingDef(lhs, _) if dict contains lhs => Term.unit
            case TermRef(target) if dict contains target => dict(target)
          }.andThen(_.tree)
        }

      /** Replaces a sequence of `symbols` in a tree with freshly named ones. */
      def refresh(symbols: Seq[u.Symbol]): u.Tree => u.Tree =
        rename(for (sym <- symbols) yield sym -> {
          if (sym.isTerm) TermSym.fresh(sym.asTerm)
          else TypeSym.fresh(sym.asType)
        })

      /** Refreshes all `symbols` in a tree that are defined within (including lambdas). */
      def refreshAll(tree: u.Tree): u.Tree =
        refresh(tree.collect {
          case TermDef(sym) => sym
          case Lambda(fun, _, _) => fun
        })(tree)

      /**
       * Replaces a sequence of term symbols with references to their `aliases`.
       * Dependent symbols are changed as well, such as children symbols with renamed owners,
       * and method symbols with renamed (type) parameters.
       */
      def rename(
        aliases: Seq[(u.Symbol, u.Symbol)],
        typeMap: Map[u.Type, u.Type] = Map.empty.withDefault(identity)
      ): u.Tree => u.Tree =
        if (aliases.isEmpty) identity
        else tree => Sym.subst(Owner.of(tree), aliases, typeMap)(tree)

      /** Replaces occurrences of `find` with `repl` in a tree. */
      def replace(find: u.Tree, repl: u.Tree): u.Tree => u.Tree =
        TopDown.break.transform {
          case tree if tree equalsStructure find => repl
        }.andThen(_.tree)

      /** Substitutes a sequence of symbol-value pairs in a tree. */
      def subst(kvs: Seq[(u.Symbol, u.Tree)]): u.Tree => u.Tree =
        subst(kvs.toMap)

      /** Substitutes a dictionary of symbol-value pairs in a tree. */
      def subst(dict: Map[u.Symbol, u.Tree]): u.Tree => u.Tree =
        if (dict.isEmpty) identity else {
          val closure = dict.values
            .flatMap(this.closure)
            .filterNot(dict.keySet)
            .map(_.name).toSet

          TopDown.break.accumulate(Attr.collect[Set, u.TermSymbol] {
            case TermDef(lhs) => lhs
          }).transform { case TermRef(target) if dict contains target =>
            dict(target)
          }.andThen { case Attr.acc(tree, defs :: _) =>
            val capture = for (d <- defs if closure(d.name)) yield d
            refresh(capture.toSeq)(tree)
          }
        }

      /** Creates a curried version of the supplied `lambda`. */
      def curry(lambda: u.Function): u.Function = lambda match {
        case Lambda(_, params, body) =>
          if (params.size <= 1) lambda else params.foldRight(body) {
            case (ParDef(lhs, _), rhs) => Lambda(Seq(lhs), rhs)
          }.asInstanceOf[u.Function]
      }

      /** Removes all (possibly nested) type ascriptions from `tree`. */
      @tailrec def unAscribe(tree: u.Tree): u.Tree = tree match {
        case TypeAscr(expr, _) => unAscribe(expr)
        case _ => tree
      }
    }

    /** The empty tree (instance independent). */
    object Empty extends Node {

      def apply(): u.Tree =
        u.EmptyTree

      def unapply(tree: u.Tree): Option[u.Tree] =
        Option(tree).filter(_.isEmpty)
    }

    /** Identifiers (for internal use). */
    private[ast] object Id extends Node {

      def apply(target: u.Symbol): u.Ident = {
        assert(is.defined(target), s"$this target is not defined")
        assert(has.nme(target),    s"$this target $target has no name")
        assert(has.tpe(target),    s"$this target $target has no type")
        assert(target.isType || is.encoded(target), s"$this target $target is not encoded")
        setType(mkIdent(target), target.info match {
          case u.NullaryMethodType(res) => res
          case tpe if tpe <:< Type.anyRef && is.stable(target) =>
            singleType(u.NoPrefix, target)
          case tpe => tpe
        })
      }

      def unapply(id: u.Ident): Option[u.Symbol] = id match {
        case Tree.With.sym(_, target) => Some(target)
        case _ => None
      }
    }

    /** Selections (for internal use). */
    private[ast] object Sel extends Node {

      def apply(target: u.Tree, member: u.Symbol): u.Select = {
        assert(is.defined(target), s"$this target is not defined")
        assert(has.tpe(target),    s"$this target has no type:\n${Tree.showTypes(target)}")
        assert(is.defined(member), s"$this member $member is not defined")
        assert(has.tpe(member),    s"$this member $member has no type")
        val mod = member.isPackageClass || member.isModuleClass
        val sym = if (mod) member.asClass.module else member
        val sel = mkSelect(target, sym)
        setType(sel, sym.infoIn(target.tpe) match {
          case u.NullaryMethodType(res) =>
            if (res <:< Type.anyRef && is.stable(sym) && is.stable(target.tpe)) {
              singleType(target.tpe, sym)
            } else res
          case tpe => tpe
        })
      }

      def unapply(sel: u.Select): Option[(u.Tree, u.Symbol)] = sel match {
        case Tree.With.sym(u.Select(target, _), member) => Some(target, member)
        case _ => None
      }
    }

    /** References. */
    object Ref extends Node {

      /**
       * Creates a type-checked reference.
       * @param target Cannot be a method or package.
       * @return `target`.
       */
      def apply(target: u.Symbol): u.Ident = {
        assert(is.defined(target), s"$this target is not defined")
        assert(!target.isPackage,  s"$this target $target cannot be a package")
        assert(!target.isMethod,   s"$this target $target cannot be a method")
        Id(target)
      }

      def unapply(ref: u.Ident): Option[u.Symbol] = ref match {
        case Id(target) if !target.isPackage && !target.isMethod => Some(target)
        case _ => None
      }
    }

    /** Member accesses. */
    object Acc extends Node {

      /**
       * Creates a type-checked member access.
       * @param target Must be a term.
       * @param member Must be a dynamic symbol.
       * @return `target.member`.
       */
      def apply(target: u.Tree, member: u.Symbol): u.Select = {
        assert(is.defined(target), s"$this target is not defined")
        assert(is.term(target),    s"$this target is not a term:\n${Tree.show(target)}")
        assert(is.defined(member), s"$this member is not defined")
        assert(!member.isStatic,   s"$this member $member cannot be static")
        assert(!is.method(member), s"$this member $member cannot be a method")
        Sel(target, member)
      }

      def unapply(acc: u.Select): Option[(u.Tree, u.Symbol)] = acc match {
        case Sel(Term(target), member) if !member.isStatic &&
          !is.method(member) => Some(target, member)
        case _ => None
      }
    }

    /** Definitions. */
    object Def extends Node {
      def unapply(tree: u.Tree): Option[u.Symbol] = for {
        tree <- Option(tree) if tree.isDef && has.sym(tree)
      } yield tree.symbol
    }
  }
}
