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
package compiler.lang.core

import compiler.Common
import compiler.lang.source.Source

import cats.std.all._
import shapeless._

import scala.Function.const

/** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
private[core] trait ANF extends Common {
  this: Source with Core =>

  import UniverseImplicits._
  import Core.{Lang => core}
  import Source.{Lang => src}

  /** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
  private[core] object ANF {

    /** The ANF transformation. */
    private lazy val anf: u.Tree => u.Tree =
      api.BottomUp.withParent.withOwner.transformWith {
        // lit | this | x
        case Attr.inh(src.Atomic(atom), _ :: parent :: _) => parent.collect {
          // Convert atoms to trivial blocks in lambdas, comprehensions and continuations.
          case src.Lambda(_, _, _) =>
          case core.Comprehension(_) =>
          case core.DefDef(_, _, _, _) =>
        }.fold(atom)(const(src.Block(expr = atom)))

        // comprehension[T] { ... }
        case Attr.none(compr @ core.Comprehension(_)) =>
          src.Block(expr = compr)

        // cont(..args) | if (cond) cont1(..args1) else cont2(..args2)
        case Attr.none(cont @ core.Continuation(_)) => cont

        // def cont(..params) = { ..stats; atom }
        case Attr.none(defn @ core.DefDef(method, tparams, paramss, AsBlock(stats, expr))) =>
          if (stats.nonEmpty) defn else {
            val pss = for (params <- paramss) yield for (core.ParDef(p, _) <- params) yield p
            core.DefDef(method, tparams, pss, src.Block(expr = expr))
          }

        // { ..stats; atom }: T
        case Attr.inh(src.TypeAscr(block @ AsBlock(stats, expr), tpe), owner :: _) =>
          if (expr.tpe =:= tpe) block else {
            val nme = api.TermName.fresh(expr.symbol)
            val lhs = api.ValSym(owner, nme, tpe)
            val rhs = core.TypeAscr(expr, tpe)
            val tmp = core.ValDef(lhs, rhs)
            val ref = core.Ref(lhs)
            src.Block(stats :+ tmp, ref)
          }

        // { ..stats; atom }.module
        case Attr.inh(src.ModuleAcc(AsBlock(stats, expr), module), owner :: _) =>
          val nme = api.TermName.fresh(module)
          val lhs = api.ValSym(owner, nme, module.info)
          val rhs = core.ModuleAcc(expr, module)
          val tmp = core.ValDef(lhs, rhs)
          val ref = core.Ref(lhs)
          src.Block(stats :+ tmp, ref)

        // target.method[..targs](...argss)
        case Attr.inh(src.DefCall(target, method, targs, argss), owner :: _) =>
          val expr = for (AsBlock(_, a) <- target) yield a
          val exprss = for (args <- argss) yield
            for (AsBlock(_, arg) <- args) yield arg

          val prefix = for {
            AsBlock(ss, _) <- target.toSeq
            stat <- ss
          } yield stat

          val suffix = for {
            args <- argss
            AsBlock(ss, _) <- args
            stat <- ss
          } yield stat

          val rhs = core.DefCall(expr, method, targs, exprss)
          val nme = api.TermName.fresh(method)
          val lhs = api.ValSym(owner, nme, rhs.tpe)
          val tmp = core.ValDef(lhs, rhs)
          val ref = core.Ref(lhs)
          src.Block(Seq.concat(prefix, suffix, Seq(tmp)), ref)

        // new cls[..targs](...argss)
        case Attr.inh(src.Inst(cls, targs, argss), owner :: _) =>
          val exprss = for (args <- argss) yield
            for (AsBlock(_, arg) <- args) yield arg

          val stats = for {
            args <- argss
            AsBlock(ss, _) <- args
            stat <- ss
          } yield stat

          val rhs = core.Inst(cls, targs, exprss)
          val nme = api.TermName.fresh(cls.typeSymbol)
          val lhs = api.ValSym(owner, nme, rhs.tpe)
          val tmp = core.ValDef(lhs, rhs)
          val ref = core.Ref(lhs)
          src.Block(stats :+ tmp, ref)

        // (params) => { ..stats; atom }
        case Attr.none(lambda @ src.Lambda(fun, _, _)) =>
          val nme = api.TermName.fresh(api.TermName.lambda)
          val lhs = api.ValSym(fun.owner, nme, lambda.tpe)
          val tmp = core.ValDef(lhs, lambda)
          val ref = core.Ref(lhs)
          src.Block(Seq(tmp), ref)

        // if ({ ..stats, cond }) thn else els
        case Attr.inh(src.Branch(AsBlock(stats, cond), thn, els), owner :: _) =>
          val rhs = src.Branch(cond, thn, els)
          val nme = api.TermName.fresh("if")
          val lhs = api.ValSym(owner, nme, rhs.tpe)
          val tmp = core.ValDef(lhs, rhs)
          val ref = core.ValRef(lhs)
          src.Block(stats :+ tmp, ref)

        // { ..outer; { ..inner; atom } }
        case Attr.none(src.Block(outer, AsBlock(inner, expr))) =>
          val stats = outer.flatMap {
            case src.ValDef(x, AsBlock(ss :+ core.ValDef(y, rhs), core.Ref(z)))
              if y == z => ss :+ core.ValDef(x, rhs)
            case src.ValDef(lhs, AsBlock(ss, rhs)) =>
              ss :+ src.ValDef(lhs, rhs)
            case src.VarDef(lhs, AsBlock(ss, rhs)) =>
              ss :+ src.VarDef(lhs, rhs)
            case src.VarMut(lhs, AsBlock(ss, rhs)) =>
              ss :+ src.VarMut(lhs, rhs)
            case src.Block(ss, src.Atomic(_)) => ss
            case src.Block(ss, stat) => ss :+ stat
            case stat => Seq(stat)
          }

          src.Block(stats ++ inner, expr)
      }._tree

    /**
     * Converts a tree into administrative normal form (ANF).
     *
     * == Postconditions ==
     *
     * - There are no name clashes (ensured with `resolveNameClashes`).
     * - Introduces dedicated symbols for sub-terms in application trees.
     * - Ensures that all function arguments are trivial identifiers.
     *
     * @return An ANF version of the input tree.
     */
    lazy val transform: u.Tree => u.Tree =
      resolveNameClashes andThen anf

    /**
     * Inlines `Ident` return expressions in blocks whenever the referred symbol is used only once.
     * The resulting tree is said to be in ''simplified ANF'' form.
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[transform]]).
     *
     * == Postconditions ==
     * - `Ident` return expressions in blocks have been inlined whenever possible.
     */
    lazy val inlineLetExprs: u.Tree => u.Tree =
      api.BottomUp.withValDefs.withValUses.transformWith {
        case Attr.syn(src.Block(stats, src.ValRef(target)), uses :: defs :: _)
          if defs.contains(target) && uses(target) == 1 =>
            val value = defs(target)
            src.Block(stats.filter(_ != value), value.rhs)
      }.andThen(_.tree)

    /**
     * Introduces `Ident` return expressions in blocks whenever the original expr is not a ref or
     * literal.The opposite of [[inlineLetExprs]].
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[transform]]).
     *
     * == Postconditions ==
     * - `Ident` return expressions in blocks have been introduced whenever possible.
     */
    lazy val uninlineLetExprs: u.Tree => u.Tree = api.TopDown
      // Accumulate all method definitions seen so far.
      .accumulate { case core.Let(_, defs, _) =>
        (for (core.DefDef(method, _, _, _) <- defs) yield method).toSet
      }.withOwner.transformWith {
        case Attr.acc(let @ core.Let(_, _, expr), local :: _)
          if isDSCF(expr)(local) => let

        case Attr.inh(core.Let(vals, defs, expr), owner :: _) =>
          val nme = api.TermName.fresh("x")
          val lhs = api.ValSym(owner, nme, expr.tpe)
          val ref = core.Ref(lhs)
          val dfn = core.ValDef(lhs, expr)
          core.Let(vals :+ dfn, defs, ref)
      }.andThen(_.tree)

    /**
     * Un-nests nested blocks.
     *
     * == Preconditions ==
     * - Except the nested blocks, the input tree is in ANF form.
     *
     * == Postconditions ==
     * - An ANF tree where all nested blocks have been flattened.
     */
    lazy val flatten: u.Tree => u.Tree =
      api.BottomUp.transform {
        case parent @ core.Let(vals, defs, expr) if hasNestedLets(parent) =>
          // Flatten nested let expressions in value position without control flow.
          val flatVals = vals.flatMap {
            case core.ValDef(lhs, core.Let(nestedVals, Seq(), nestedExpr)) =>
              (nestedVals, nestedExpr) match {
                // Match: { ..vals; val x = expr; x }
                case (prefix :+ core.ValDef(x, rhs), core.ValRef(y))
                  if x == y => prefix :+ core.ValDef(lhs, rhs)
                // Match: { ..vals; expr }
                case (prefix, rhs) =>
                  prefix :+ core.ValDef(lhs, rhs)
              }
            case value =>
              Some(value)
          }

          // Flatten nested let expressions in expr position without control flow.
          val (exprVals, flatDefs, flatExpr) = expr match {
            case core.Let(nestedVals, nestedDefs, nestedExpr) =>
              (nestedVals, nestedDefs, nestedExpr)
            case _ =>
              (Seq.empty, defs, expr)
          }

          val (trimmedVals, trimmedExpr) = trimVals(flatVals ++ exprVals, flatExpr)
          core.Let(trimmedVals, flatDefs, trimmedExpr)
      }.andThen(_.tree)

    // ---------------
    // Helper methods
    // ---------------

    // Handle degenerate cases where the suffix is of the form:
    // { ..vals; val x2 = x3; val x1 = x2; x1 }
    private def trimVals(vals: Seq[u.ValDef], expr: u.Tree): (Seq[u.ValDef], u.Tree) =
      (Seq.empty, vals.foldRight(expr, 0) {
        case (core.ValDef(x, rhs @ core.Atomic(_)), (core.ValRef(y), n))
          if x == y => (rhs, n + 1)
        case (_, (rhs, n)) =>
          //scalastyle:off
          return (vals.dropRight(n), rhs)
          //scalastyle:on
      }._1)

    /** Does the input `let` block contain nested `let` expressions? */
    private def hasNestedLets(let: u.Block): Boolean = {
      def inStats = let.stats.exists {
        case core.ValDef(_, core.Let(_, Seq(), _)) => true
        case _ => false
      }
      def inExpr = let.expr match {
        case core.Let(_, _, _) => true
        case _ => false
      }
      inStats || inExpr
    }

    /** Checks whether the given expression represents direct-style control-flow. */
    private def isDSCF(expr: u.Tree)(isLocal: u.MethodSymbol => Boolean): Boolean = expr match {
      //@formatter:off
      // 1a) branch with one local method call and one atomic
      case core.Branch(core.Atomic(_),
        core.DefCall(_, thn, _, _),
        core.Atomic(_)
      ) => isLocal(thn)
      // 1b) reversed version of 1a
      case core.Branch(core.Atomic(_),
        core.Atomic(_),
        core.DefCall(_, els, _, _)
      ) => isLocal(els)
      // 2) branch with two local method calls
      case core.Branch(core.Atomic(_),
        core.DefCall(_, thn, _, _),
        core.DefCall(_, els, _, _)
      ) => isLocal(thn) && isLocal(els)
      // 3) simple local method call
      case core.DefCall(_, method, _, _) => isLocal(method)
      // 4) simple atomic
      case core.Atomic(_) => true
      // 5) anything else
      case _ => false
      //@formatter:on
    }

    /** Extract `(stats, expr)` from blocks and non-blocks. */
    object AsBlock {
      def unapply(tree: u.Tree): Option[(Seq[u.Tree], u.Tree)] = tree match {
        case src.Block(stats, expr) => Some(stats, expr)
        case expr => Some(Seq.empty, expr)
      }
    }
  }
}
