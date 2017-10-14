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
package compiler.lang.comprehension

import compiler.Common
import compiler.lang.core.Core
import util.Monoids

import shapeless._

/** Resugaring and desugaring of comprehension syntax. */
private[comprehension] trait ReDeSugar extends Common {
  self: Core =>

  import Monoids._

  import Comprehension.asLet
  import Core.{Lang => core}
  import UniverseImplicits._

  private[comprehension] object ReDeSugar {

    /**
     * Resugars monad ops into mock-comprehension syntax.
     *
     * == Preconditions ==
     *
     * - The input tree is in ANF.
     *
     * == Postconditions ==
     *
     * - A tree where monad operations are resugared into one-generator mock-comprehensions.
     *
     * @param monad The symbol of the monad to be resugared.
     * @return A resugaring transformation for that particular monad.
     */
    def resugar(monad: u.Symbol) = TreeTransform("ReDeSugar.resugar", {
      // Construct comprehension syntax helper for the given monad
      val cs = Comprehension.Syntax(monad)
      // Handle the case when a lambda is defined externally
      def lookup(f: u.TermSymbol, owner: u.Symbol,
        lambdas: Map[u.TermSymbol, (u.TermSymbol, u.Tree)]
      ) = lambdas.get(f).map { case (arg, body) =>
        val sym = api.Sym.With(arg)(flg = u.NoFlags).asTerm
        sym -> api.Tree.rename(Seq(arg -> sym))(body)
      }.getOrElse {
        val nme = api.TermName.fresh()
        val tpe = f.info.dealias.widen.typeArgs.head
        val arg = api.ValSym(owner, nme, tpe)
        val tgt = Some(core.ValRef(f))
        val app = f.info.member(api.TermName.app).asMethod
        arg -> core.DefCall(tgt, app, argss = Seq(Seq(core.ValRef(arg))))
      }

      api.BottomUp.withOwner.accumulate(Attr.group {
        // Accumulate a LHS -> (arg, body) Map from lambdas
        case core.ValDef(lhs, core.Lambda(_, Seq(core.ParDef(arg, _)), body)) =>
          lhs -> (arg, body)
      })(overwrite).transformWith { // Re-sugar comprehensions
        case Attr(cs.Map(xs, core.Ref(f)), lambdas :: _, owner :: _, _) =>
          val (sym, body) = lookup(f, owner, lambdas)
          api.Owner.at(owner)(
            cs.Comprehension(Seq(
              cs.Generator(sym, asLet(xs))),
              cs.Head(asLet(body))))

        case Attr(cs.FlatMap(xs, core.Ref(f)), lambdas :: _, owner :: _, _) =>
          val (sym1, body1) = lookup(f, owner, lambdas)
          val (sym2, body2) = {
            val nme = api.TermName("g")
            val tpe = api.Type.arg(1, body1.tpe)
            val sym = api.TermSym(sym1.owner, nme, tpe)
            sym -> core.Ref(sym)
          }
          api.Owner.at(owner)(
            cs.Comprehension(Seq(
              cs.Generator(sym1, asLet(xs)),
              cs.Generator(sym2, asLet(body1))),
              cs.Head(asLet(body2))))

        case Attr(cs.WithFilter(xs, core.Ref(p)), lambdas :: _, owner :: _, _) =>
          val (sym, body) = lookup(p, owner, lambdas)
          api.Owner.at(owner)(
            cs.Comprehension(Seq(
              cs.Generator(sym, asLet(xs)),
              cs.Guard(asLet(body))),
              cs.Head(core.Let(expr = core.Ref(sym)))))
      }._tree andThen Core.dce
    })

    /**
     * Desugars mock-comprehension syntax into monad ops.
     *
     * == Preconditions ==
     *
     * - An ANF tree with mock-comprehensions.
     *
     * == Postconditions ==
     *
     * - A tree where mock-comprehensions are desugared into comprehension operator calls.
     *
     * @param monad The symbol of the monad syntax to be desugared.
     * @return A desugaring transformation for that particular monad.
     */
    def desugar(monad: u.Symbol) = TreeTransform("ReDeSugar.desugar", {
      // construct comprehension syntax helper for the given monad
      val cs = Comprehension.Syntax(monad)
      api.TopDown.withOwner.transformWith {
        // Match: `for { x <- { $vals; $rhs}; $qs*; } yield $expr`
        case Attr.inh(
          cs.Comprehension(Seq(cs.Generator(x, core.Let(vals, Seq(), core.Ref(rhs))), qs@_*), cs.Head(expr)),
          owner :: _) =>

          val (guards, rest) = qs.span {
            case cs.Guard(_) => true
            case _ => false
          }

          // Accumulate filters in a prefix of values and keep track of the tail value symbol
          val (tail, prefix) = guards.foldLeft(rhs, Vector.empty[u.ValDef]) {
            case ((curSym, curPre), cs.Guard(pred)) =>
              val curRef = core.Ref(curSym)
              val lambda = core.Lambda(Seq(x), pred)
              val funNme = api.TermName.fresh("guard")
              val funSym = api.ValSym(owner, funNme, lambda.tpe)
              val funRef = core.Ref(funSym)
              val funVal = api.ValDef(funSym, lambda)
              val tmpNme = api.TermName.fresh(cs.WithFilter.symbol)
              val tmpSym = api.ValSym(owner, tmpNme, curSym.info.widen)
              val tmpVal = api.ValDef(tmpSym, cs.WithFilter(curRef)(funRef))
              (tmpSym, curPre ++ Seq(funVal, tmpVal))
          }

          val tailRef = core.Ref(tail)
          expr match {
            case core.Let(Seq(), Seq(), core.Ref(`x`)) =>
              // Trivial head expression consisting of the matched sym 'x'
              // Omit the resulting trivial mapper
              core.Let(vals ++ prefix, Seq.empty, tailRef)

            case _ =>
              // Append a map or a flatMap to the result depending on
              // the size of the residual qualifier sequence 'qs'
              val (op, body) = if (rest.isEmpty) (cs.Map, expr)
                else (cs.FlatMap, cs.Comprehension(rest, cs.Head(expr)))

              val funNme = api.TermName.fresh(api.TermName.lambda)
              val funTpe = api.Type.fun(Seq(x.info), body.tpe.widen)
              val funSym = api.ValSym(owner, funNme, funTpe)
              val tmpNme = api.TermName.fresh()
              val tmpSym = api.ValSym(funSym, tmpNme, body.tpe.widen)
              val tmpRef = core.Ref(tmpSym)
              val tmpVal = core.ValDef(tmpSym, body)
              val lambda = core.Lambda(Seq(x), core.Let(Seq(tmpVal), Seq.empty, tmpRef))
              val funVal = core.ValDef(funSym, lambda)

              val opRhs = op(tailRef)(core.Ref(funSym))
              val opSym = api.ValSym(owner, op.symbol.name, opRhs.tpe)
              val opVal = core.ValDef(opSym, opRhs)

              val allVals = Seq.concat(vals, prefix, Seq(funVal, opVal))
              core.Let(allVals, Seq.empty, core.Ref(opSym))
          }
      }._tree.andThen(Core.unnest)
    })
  }
}
