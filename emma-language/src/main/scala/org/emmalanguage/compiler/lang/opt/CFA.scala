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
package compiler.lang.opt

import compiler.Common
import compiler.lang.core.Core
import util.Monoids

import cats.std.all._
import shapeless._

/** Control-flow graph analysis (CFA). */
private[compiler] trait CFA extends Common {
  this: Core =>

  /** Control-flow graph analysis (CFA). */
  private[compiler] object CFA {

    import UniverseImplicits._
    import Core.{Lang => core}
    import GraphRepresentation.phi

    /** A control-flow graph. */
    case class Graph(
      values:   Map[u.TermSymbol, u.ValDef],
      dataflow: Map[u.TermSymbol, Set[u.TermSymbol]],
      contains: Map[u.TermSymbol, Set[u.TermSymbol]])

    private val module = Some(core.Ref(GraphRepresentation.module))

    /** Control-flow graph analysis (CFA). */
    def graph(monad: u.ClassSymbol = API.bagSymbol): u.Tree => Graph = {
      val cs = new Comprehension.Syntax(monad)
      api.TopDown.skipTypeTrees.withBindUses.withBindDefs
        .inherit { // Enclosing comprehension / lambda, if any
          case core.ValDef(lhs, cs.Comprehension(_, _), _) => Option(lhs)
          case core.ValDef(lhs, core.Lambda(_, _, _), _) => Option(lhs)
        } (Monoids.right(None)).inherit { // Accessible methods
          case core.Let(_, defs, _) => defs.map(api.TermSym.of).toSet
        }.accumulateWith[Map[u.TermSymbol, Set[u.TermSymbol]]] { // Dataflow edges
          case Attr.syn(core.ValDef(lhs, _, _), defs :: uses :: _) =>
            Map(lhs -> uses.keySet.diff(defs.keySet).filterNot(_.isStatic))
        }.accumulateWith[Map[u.TermSymbol, Set[u.TermSymbol]]] { // Contains edges
          case Attr.inh(core.ValDef(lhs, _, _), _ :: Some(encl) :: _) =>
            Map(encl -> Set(lhs))
        } (Monoids.merge).accumulateWith[Map[u.TermSymbol, Vector[u.Tree]]] { // Phi choices
          case Attr.inh(core.DefCall(None, method, _, argss@_*), local :: _) if local(method) => {
            for ((param, arg) <- method.paramLists.flatten zip argss.flatten)
              yield param.asTerm -> Vector(arg)
          }.toMap
        } (Monoids.merge).traverseAll.andThen {
          case Attr(tree, choices :: contains :: dataflow :: _, _, defs :: _) =>
            val values = defs.collect {
              case value @ (api.ValSym(_), _) => value
              case (_, core.ParDef(lhs, _, flags)) if choices.contains(lhs) =>
                val tpe = api.Type.of(lhs)
                val rhs = core.DefCall(module)(phi, tpe)(choices(lhs))
                lhs -> core.ParDef(lhs, rhs, flags)
            }

            Graph(values, dataflow, contains)
        }
    }
  }
}
