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
package compiler.lang.cogadb

import compiler.Common
import compiler.lang.core.Core

import util.Monoids._

import shapeless._

trait CoGaUDFSupport extends Common {
  self: Core =>

  import Core.{Lang => core}
  import UniverseImplicits._

  private object AttrRefAPI extends ClassAPI {
    //@formatter:off
    lazy val sym = api.Sym[ast.AttrRef].asClass
    lazy val ops = Set.empty[u.MethodSymbol]
    //@formatter:on
  }

  private object AttrRef$API extends ModuleAPI {
    //@formatter:off
    lazy val sym = api.Sym[ast.AttrRef.type].asModule
    val apply    = op("apply")
    lazy val ops = Set(apply)
    //@formatter:on
  }

  object CoGaUDFSupport {
    lazy val apply: u.Tree => u.Tree = {
      case core.Lambda(sym, params, body@core.Let(_, Seq(), _)) =>
        xfrm(sym.owner, params.map(_.symbol.asTerm).toSet)(body)
      case tree =>
        tree
    }

    /** Attempts to transform an Emma Core Let expression to Spark's `Column` expression API. */
    private def xfrm(owner: u.Symbol, params: Set[u.TermSymbol]) = api.TopDown.break.withValDefs
      .synthesize(Attr.group {
        case api.ValDef(lhs, _) => lhs -> api.TermSym.apply(owner, lhs.name, AttrRefAPI.tpe)
      })(overwrite)
      .transformWith {
        case Attr.syn(let@core.Let(vals, Seq(), expr), attr :: _) =>
          val vals1 = vals.map({
            case core.ValDef(lhs, rhs) => rhs match {
              case core.DefCall(Some(core.Ref(tgt)), method, Seq(), Seq())
                if method.isGetter && (params contains tgt) =>

                // $x.$field where $x is a parameter of the enclosing lambda
                val ref = core.Ref(AttrRef$API.sym)
                val unk = core.Lit("unknown")
                val fld = core.Lit(method.name.toString)
                val ver = core.Lit(1)
                val ags = Seq(Seq(unk, fld, fld, ver))
                val rhs = core.DefCall(Some(ref), AttrRef$API.apply, Seq(), ags)
                Some(core.ValDef(attr(lhs), rhs))

              case _ => None
            }
            case _ => None
          })

          val expr1 = expr match {
            case core.Ref(sym) if attr contains sym => Some(core.Ref(attr(sym)))
            case _ => None
          }

          if ((expr1 +: vals1).exists(_.isEmpty)) let
          else core.Let(vals1.flatten, Seq(), expr1.get)
      }._tree
  }


}
