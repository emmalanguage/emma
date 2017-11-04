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

import shapeless._

import scala.Function.const

/** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
private[core] trait ANF extends Common {
  self: Source with Core =>

  import Core.{Lang => core}
  import Source.{Lang => src}
  import API.{ComprehensionSyntax=>cs}
  import UniverseImplicits._

  /** Administrative Normal Form (ANF) bypassing control-flow and for-comprehensions. */
  private[core] object ANF {

    /** The ANF transformation. */
    private lazy val anf = TreeTransform("ANF.anf",
      api.BottomUp.withParent.withOwner.transformWith {
        // lit | this | x
        case Attr.inh(src.Atomic(atom), _ :: parent :: _) => parent.collect {
          // Convert atoms to trivial blocks in lambdas, comprehensions and continuations.
          case src.Lambda(_, _, _) =>
          case core.DefCall(Some(core.Ref(cs.sym)), _, _, _) =>
          case core.DefDef(_, _, _, _) =>
        }.fold(atom)(const(src.Block(expr = atom)))

        // comprehension[T] { ... }
        case Attr.inh(compr @ core.DefCall(Some(core.Ref(cs.sym)), cs.comprehension, _, _), owner :: _) =>
          val valSym = api.ValSym(owner, api.TermName.fresh("compr"), compr.tpe)
          src.Block(Seq(core.ValDef(valSym, compr)), core.Ref(valSym))

        // generator { ... } | guard { ... } | head { ... }
        case Attr.none(compr @ core.DefCall(Some(core.Ref(cs.sym)), cs.generator | cs.guard | cs.head, _, _)) =>
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
        case Attr.inh(src.TermAcc(AsBlock(stats, expr), module), owner :: _) =>
          val rhs = core.TermAcc(expr, module)
          val nme = api.TermName.fresh(module)
          val lhs = api.ValSym(owner, nme, rhs.tpe)
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
          val nme = api.TermName.fresh("anf")
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
          val nme = api.TermName.fresh("anf")
          val lhs = api.ValSym(owner, nme, rhs.tpe)
          val tmp = core.ValDef(lhs, rhs)
          val ref = core.Ref(lhs)
          src.Block(stats :+ tmp, ref)

        // (params) => { ..stats; atom }
        case Attr.none(lambda @ src.Lambda(fun, _, _)) =>
          val nme = api.TermName.fresh("fun")
          val lhs = api.ValSym(fun.owner, nme, lambda.tpe)
          val tmp = core.ValDef(lhs, lambda)
          val ref = core.Ref(lhs)
          src.Block(Seq(tmp), ref)

        // if ({ ..stats, cond }) thn else els
        case Attr.inh(src.Branch(AsBlock(stats, cond), thn, els), owner :: _) =>
          val rhs = src.Branch(cond, thn, els)
          val nme = api.TermName.fresh("anf")
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
      }._tree.andThen(api.Owner.atEncl))

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
    lazy val transform = TreeTransform("ANF.transform", Seq(
      resolveNameClashes,
      anf
    ))

    /**
     * Un-nests nested blocks.
     *
     * == Preconditions ==
     * - Except the nested blocks, the input tree is in ANF form.
     *
     * == Postconditions ==
     * - An ANF tree where all nested blocks have been flattened.
     */
    lazy val unnest = TreeTransform("ANF.unnest",
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
      }._tree.andThen(api.Owner.atEncl))

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

    /** Extract `(stats, expr)` from blocks and non-blocks. */
    object AsBlock {
      def unapply(tree: u.Tree): Option[(Seq[u.Tree], u.Tree)] = tree match {
        case src.Block(stats, expr) => Some(stats, expr)
        case expr => Some(Seq.empty, expr)
      }
    }
  }
}
