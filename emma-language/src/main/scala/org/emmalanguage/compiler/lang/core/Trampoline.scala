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
import util.Monoids._

import shapeless._

import scala.util.control.TailCalls.TailRec

/** Trampolining tail calls to avoid stack overflow. */
private[core] trait Trampoline extends Common {
  self: Core =>

  /** Trampolining tail calls to avoid stack overflow. */
  private[core] object Trampoline {

    import Core.{Lang => core}
    import UniverseImplicits._
    import u.internal.flags

    private val module = u.rootMirror.staticModule("scala.util.control.TailCalls")
    private val TailCalls = Some(core.Ref(module))
    private val done = module.info.member(api.TermName("done")).asMethod
    private val tailcall = module.info.member(api.TermName("tailcall")).asMethod
    private val result = api.Type[TailRec[Nothing]].typeConstructor
      .member(api.TermName("result")).asMethod

    /**
     * Wraps return values and tail calls in a trampoline, finally returning its result.
     *
     * == Preconditions ==
     * - The input tree is in DSCF (see [[Core.dscf]]).
     *
     * == Postconditions ==
     * - All return values and tail calls are wrapped in a trampoline.
     * - The ANF shape of the input is NOT preserved.
     */
    // Unsafe: Return type of methods changes to TailRec[OriginalType].
    lazy val transform = TreeTransform("Trampoline.transform", api.BottomUp.unsafe
      .withAncestors.inherit { // Local method definitions.
        case core.Let(_, defs, _) =>
          (for (core.DefDef(method, tparams, paramss, _) <- defs) yield {
            val (own, nme, pos) = (method.owner, method.name, method.pos)
            val flg = flags(method)
            val pss = paramss.map(_.map(_.symbol.asTerm))
            val res = api.Type.kind1[TailRec](method.info.finalResultType)
            val ans = method.annotations
            method -> api.DefSym(own, nme, tparams, pss, res, flg, pos, ans)
          }).toMap
      } (overwrite).transformWith {
        // Return position in a method definition, wrap in trampoline.
        case Attr.inh(tree, local :: (_ :+ (_: u.DefDef) :+ core.Let(_, _, expr)) :: _)
          if tree == expr => wrap(expr, local)

        // Local method definition, returns a trampoline.
        case Attr.inh(core.DefDef(method, tparams, paramss, core.Let(vals, defs, expr)),
          local :: _) =>
          val pss = paramss.map(_.map(_.symbol.asTerm))
          core.DefDef(local(method), tparams, pss, core.Let(vals, defs, expr))

        // Local method call outside, retrieve the trampoline result.
        case Attr.inh(core.DefCall(None, cont, targs, argss), local :: ancestors :: _)
          if local.contains(cont) && ancestors.forall {
            case _: u.DefDef => false
            case _ => true
          } => core.DefCall(Some(core.DefCall(None, local(cont), targs, argss)), result)
      }.andThen(_.tree))

    /** Wraps the return value / tail call of a method in a trampoline. */
    private def wrap(expr: u.Tree, local: Map[u.MethodSymbol, u.MethodSymbol]): u.Tree =
      expr match {
        // Wrap both branches.
        case core.Branch(cond, thn, els) =>
          core.Branch(cond, wrap(thn, local), wrap(els, local))

        // Wrap a tail call.
        case core.DefCall(None, cont, targs, argss) if local.contains(cont) =>
          val Res = cont.info.finalResultType
          core.DefCall(TailCalls, tailcall, Seq(Res),
            Seq(Seq(core.DefCall(None, local(cont), targs, argss))))

        // Wrap a return value.
        case _ =>
          core.DefCall(TailCalls, done, Seq(expr.tpe), Seq(Seq(expr)))
      }
  }
}
