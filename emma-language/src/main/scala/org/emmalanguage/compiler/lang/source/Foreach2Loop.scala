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
package compiler.lang.source

import compiler.Common

import shapeless._

import scala.collection.generic.CanBuildFrom
import scala.collection.generic.FilterMonadic

/** Eliminates `foreach` calls and replaces them with `while` loops. */
private[source] trait Foreach2Loop extends Common {
  self: Source =>

  import Source.{Lang => src}
  import UniverseImplicits._
  import api.TermName.fresh

  /**
   * Converts a `foreach` method call to a `while` loop:
   *
   * {{{
   *   for (x <- xs) println(x)
   *   // equivalently
   *   xs foreach println
   *   // becomes
   *   {
   *     val iter = xs.toIterator
   *     var x = null
   *     while (iter.hasNext) {
   *       x = iter.next()
   *       println(x)
   *     }
   *   }
   * }}}
   */
  private[source] object Foreach2Loop {

    /** The Foreach2Loop transformation. */
    lazy val transform = TreeTransform("Foreach2Loop.transform", {
      val asInst = api.Type.any.member(api.TermName("asInstanceOf")).asTerm
      api.BottomUp.withOwner.withVarDefs.withAssignments
        .transformWith {
          case Attr.all(
            src.DefCall(xsOpt @ Some(xs), method, _,
              Seq(Seq(src.Lambda(_, Seq(src.ParDef(arg, _)), body)))),
            _, owner :: _, muts :: defs :: _
          ) if {
            def isForeach = method == FM.foreach || method == api.Sym.foreach
            def overridesForeach = method.overrides.contains(api.Sym.foreach)
            def modifiesClosure = !muts.keySet.subsetOf(defs.keySet)
            (isForeach || overridesForeach) && modifiesClosure
          } =>

          val elem = api.VarSym(owner, fresh(arg), arg.info)
          val (trav, tpe) = if (method == FM.foreach) {
            val Repr = api.Type.arg(2, xs.tpe)
            val CBF = api.Type.kind3[CanBuildFrom](Repr, arg.info, Repr)
            val cbf = inferImplicit(CBF).map(unQualifyStatics)
            assert(cbf.isDefined, s"Cannot infer implicit value of type `$CBF`")
            val x = api.ParSym(owner, fresh("x"), arg.info)
            val id = src.Lambda(Seq(x), api.ParRef(x))
            val call = src.DefCall(xsOpt, FM.map, Seq(arg.info, Repr), Seq(Seq(id), Seq(cbf.get)))
            (Some(call), Repr)
          } else (xsOpt, xs.tpe)
          val toIter = tpe.member(api.TermName("toIterator")).asTerm
          val Iter = toIter.infoIn(tpe).finalResultType
          val iter = api.ValSym(owner, fresh("iter"), Iter)
          val target = Some(src.ValRef(iter))
          val hasNext = Iter.member(api.TermName("hasNext")).asTerm
          val next = Iter.member(api.TermName("next")).asTerm
          val mut = src.VarMut(elem, src.DefCall(target, next, Seq(), Seq(Seq.empty)))

          src.Block(
            Seq(
              src.ValDef(iter,
                src.DefCall(trav, toIter)),
              src.VarDef(elem,
                src.DefCall(Some(src.Lit(null)), asInst, Seq(arg.info))),
              src.While(
                src.DefCall(target, hasNext),
                api.Tree.rename(Seq(arg -> elem))(body match {
                  case src.Block(stats, expr) => src.Block(mut +: stats, expr)
                  case _ => src.Block(Seq(mut), body)
                }))),
            api.Term.unit)
      }.andThen(_.tree)
    })

    /** [[FilterMonadic]]. */
    private object FM {
      val tpe = api.Type[FilterMonadic[Nothing, Nothing]]
      val foreach = tpe.member(api.TermName("foreach")).asMethod
      val map = tpe.member(api.TermName("map")).asMethod
    }
  }

}
