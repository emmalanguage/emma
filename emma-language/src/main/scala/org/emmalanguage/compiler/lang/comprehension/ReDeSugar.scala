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
  self: Core with Comprehension =>

  import Monoids._
  import UniverseImplicits._
  import Comprehension.asLet
  import Core.{Lang => core}

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
    def resugar(monad: u.Symbol): u.Tree => u.Tree = {
      // Construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad)

      // Handle the case when a lambda is defined externally
      def lookup(f: u.TermSymbol, owner: u.Symbol,
        lambdas: Map[u.TermSymbol, (u.TermSymbol, u.Tree)]) = {

        lambdas.get(f).map { case (arg, body) =>
          val sym = api.Sym.With(arg)(flg = u.NoFlags).asTerm
          sym -> api.Tree.rename(arg -> sym)(body)
        }.getOrElse {
          val nme = api.TermName.fresh("x")
          val tpe = api.Type.result(f.info)
          val arg = api.ValSym(owner, nme, tpe)
          val tgt = Some(core.ValRef(f))
          val app = f.info.member(api.TermName.app).asMethod
          arg -> core.DefCall(tgt)(app)(Seq(core.ValRef(arg)))
        }
      }

      api.TopDown.withOwner
        // Accumulate a LHS -> (arg, body) Map from lambdas
        .accumulate(Attr.group {
          case core.ValDef(lhs, core.Lambda(_, Seq(core.ParDef(arg, _, _)), body), _) =>
            lhs -> (arg, body)
        })
        // Re-sugar comprehensions
        .transformWith {
          case Attr(cs.Map(xs, core.Ref(f)), lambdas :: _, owner :: _, _) =>
            val (sym, body) = lookup(f, owner, lambdas)
            cs.Comprehension(
              Seq(
                cs.Generator(sym, asLet(xs))),
              cs.Head(asLet(body)))

          case Attr(cs.FlatMap(xs, core.Ref(f)), lambdas :: _, owner :: _, _) =>
            val (sym, body) = lookup(f, owner, lambdas)
            cs.Flatten(
              core.Let()()(
                cs.Comprehension(
                  Seq(
                    cs.Generator(sym, asLet(xs))),
                  cs.Head(asLet(body)))))

          case Attr(cs.WithFilter(xs, core.Ref(p)), lambdas :: _, owner :: _, _) =>
            val (sym, body) = lookup(p, owner, lambdas)
            cs.Comprehension(
              Seq(
                cs.Generator(sym, asLet(xs)),
                cs.Guard(asLet(body))),
              cs.Head(core.Let()()(core.Ref(sym))))
        }.andThen(_.tree).andThen(Core.dce)
    }

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
    def desugar(monad: u.Symbol): u.Tree => u.Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad)

      val desugarTransform = api.TopDown.transform {
        // Match: `for { x <- { $vals; $rhs}; $qs*; } yield $expr`
        //@formatter:off
        case cs.Comprehension(
          Seq(
            cs.Generator(x, core.Let(vals, Seq(), core.Ref(rhs))),
            qs@_*),
          cs.Head(expr)) =>
          //@formatter:on

          val (guards, rest) = qs.span {
            case cs.Guard(_) => true
            case _ => false
          }

          // Accumulate filters in a prefix of values and keep track of the tail value symbol
          val (tail, prefix) = guards.foldLeft(rhs, Vector.empty[u.ValDef]) {
            case ((currSym, currPfx), cs.Guard(p)) =>
              val currRef = core.Ref(currSym)
              val funcRhs = core.Lambda(x)(p)
              val funcNme = api.TermName.fresh("guard")
              val funcSym = api.TermSym.free(funcNme, funcRhs.tpe)
              val funcRef = core.Ref(funcSym)
              val funcVal = api.ValDef(funcSym, funcRhs)
              val nextNme = api.TermName.fresh(cs.WithFilter.symbol)
              val nextSym = api.TermSym.free(nextNme, currSym.info)
              val nextVal = api.ValDef(nextSym, cs.WithFilter(currRef)(funcRef))
              (nextSym, currPfx :+ funcVal :+ nextVal)
          }

          val tailRef = core.Ref(tail)
          expr match {
            case core.Let(Seq(), Seq(), core.Ref(`x`)) =>
              // Trivial head expression consisting of the matched sym 'x'
              // Omit the resulting trivial mapper
              core.Let(vals ++ prefix: _*)()(tailRef)

            case _ =>
              // Append a map or a flatMap to the result depending on
              // the size of the residual qualifier sequence 'qs'
              val (op, body) =
                if (rest.isEmpty) (cs.Map, expr)
                else (cs.FlatMap, cs.Comprehension(rest, cs.Head(expr)))

              val fnRhs = core.Lambda(x)({
                val bodyNme = api.TermName.fresh("x")
                val bodySym = api.TermSym.free(bodyNme, body.tpe)
                core.Let(core.ValDef(bodySym, body))()(core.Ref(bodySym))
              })
              val fnNme = api.TermName.fresh(api.TermName.lambda)
              val fnSym = api.TermSym.free(fnNme, fnRhs.tpe)
              val fnVal = core.ValDef(fnSym, fnRhs)

              val opRhs = op(tailRef)(core.Ref(fnSym))
              val opSym = api.TermSym.free(op.symbol.name, opRhs.tpe)
              val opVal = core.ValDef(opSym, opRhs)

              core.Let(vals ++ prefix ++ Seq(fnVal, opVal): _*)()(core.Ref(opSym))
          }

        // Match: `flatten { $vals; $defs; for { $qs } yield $head }`
        case cs.Flatten(core.Let(vals, defs, cs.Comprehension(qs, cs.Head(hd)))) =>
          val genNme = api.TermName.fresh("x")
          val genSym = api.TermSym.free(genNme, api.Type.arg(1, hd.tpe))
          // Return: { $vals; $defs; for { $qs; x <- $head } yield x }
          core.Let(vals: _*)(defs: _*)(cs.Comprehension(
            qs :+ cs.Generator(genSym, hd),
            cs.Head(core.Let()()(core.Ref(genSym)))
          ))
      }

      desugarTransform andThen (_.tree) andThen Core.flatten
    }
  }
}
