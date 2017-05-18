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

import compiler.CoGaDBCompiler

import util.Monoids._
import shapeless._

import scala.collection.breakOut


private[compiler] trait CoGaDBUDFSupport {
  self: CoGaDBCompiler =>

  import API.DataBag
  import Core.{Lang => core}
  import CoGaDBAPI.Ntv
  import CoGaDBAPI.Ops
  import UniverseImplicits._


  object CoGaUDFSupport {

    lazy val specializeOps: u.Tree => u.Tree = tree => {
      val cfg = ControlFlow.cfg(tree)

      val isSpecializableUseFor = (f: u.TermSymbol, vd: u.ValDef) => vd.rhs match {
        case core.DefCall(_, m, _, Seq(Seq(core.Ref(`f`))))
          if m == DataBag.map || m == DataBag.withFilter => true
        case core.DefCall(Some(ops), Ops.equiJoin, _, JoinArgs(kx, ky, _, _))
          if f == kx || f == ky => true
        case _ => false
      }


      api.BottomUp
        .inherit {
          // accumulate lambdas in scope that can be specialized
          case core.Let(vals, _, _) =>
            (for {
              core.ValDef(x, f@core.Lambda(_, _, _)) <- vals
              h = specializeLambda(f)
              if h != f
              xUses = cfg.data.outEdges(x).flatMap(e => cfg.data.label(e.to))
              if xUses.forall(vd => isSpecializableUseFor(x, vd))
              y = api.TermSym(x.owner, x.name, h.tpe)
            } yield x -> core.ValDef(y, h)) (breakOut): Map[u.TermSymbol, u.Tree]
          case _ =>
            Map.empty[u.TermSymbol, u.Tree]
        }(overwrite)
        .transformWith {
          case Attr.inh(vd@core.ValDef(lhs, rhs), sp /* specialized lambdas */ :: _) =>
            val mapFun = (f: u.TermSymbol) => core.Ref(sp(f).symbol.asTerm)
            if (sp contains lhs) sp(lhs)
            else rhs match {
              // specialize `DataBag.withFilter` as `SparkOps.Native.select`
              /*case core.DefCall(Some(xs), DataBag.withFilter, _, Seq(Seq(core.Ref(p))))
                if sp contains p =>
                val tgt = Ntv.ref
                val met = Ntv.select
                val tas = Seq(api.Type.arg(1, xs.tpe))
                val ass = Seq(Seq(mapFun(p)), Seq(xs))
                core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass))*/
              // specialize `DataBag.map` as `SparkOps.Native.project`
              case core.DefCall(Some(xs), DataBag.map, Seq(t), Seq(Seq(core.Ref(f))))
                if sp contains f =>
                val tgt = Ntv.ref
                val met = Ntv.project
                val tas = Seq(api.Type.arg(1, xs.tpe), t)
                val ass = Seq(Seq(mapFun(f)), Seq(xs))
                core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass))
              // specialize `SparkOps.equiJoin` as `SparkOps.Native.equiJoin`
              case core.DefCall(Some(ops), Ops.equiJoin, tas, JoinArgs(kx, ky, xs, ys))
                if (sp contains kx) && (sp contains ky) =>
                val tgt = Ntv.ref
                val met = Ntv.equiJoin
                val ass = Seq(Seq(mapFun(kx), mapFun(ky)), Seq(core.Ref(xs), core.Ref(ys)))
                core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass))
              case _ =>
                vd
            }
        }(tree).tree
    }


    lazy val specializeLambda: u.Tree => u.Tree = {
      case lambda@core.Lambda(_, Seq(core.ParDef(p, _)), core.Let(vals, Seq(), core.Ref(r))) =>

        val valOf = vals.map(vd => {
          vd.symbol.asTerm -> vd.rhs
        })(breakOut): Map[u.TermSymbol, u.Tree]

        def isProductApply(x: u.TermSymbol): Boolean = valOf.get(x) match {
          case Some(core.DefCall(Some(core.Ref(t)), method, _, Seq(args))) if method.isSynthetic
            && method.name == api.TermName.app
            && t.companion.info.baseClasses.contains(api.Sym[Product]) => true
          case _ => false
        }

        //println(valOf)
        val tgt = api.Sym[ast.AttrRef.type].asModule
        val app = tgt.info.member(api.TermName.app).asMethod

        if (valOf.contains(r)) {
          val mapSym = Map(
            p -> api.TermSym.free(p.name, api.Type[String])
          ) ++ (vals.map(vd => {
            val x = vd.symbol.asTerm
            //println(x)

            val W =
              if (x == r && isProductApply(x)) api.Type.kind1[Seq](api.Type[ast.AttrRef])
              else api.Type[ast.AttrRef]

            val w = api.TermSym.free(x.name, W)
            x -> w
          })(breakOut): Map[u.TermSymbol, u.TermSymbol])

          //println(mapSym)
          val mapArgs = (args: Seq[u.Tree]) => args map {
            case core.Ref(z) if mapSym contains z => core.Ref(mapSym(z))
            case arg => arg
          }

          val vals1 = for (core.ValDef(x, rhs) <- vals) yield rhs match {
            // translate projections
            case core.DefCall(Some(core.Ref(z)), method, Seq(), Seq())
              if method.isGetter =>

              //println(test)
              mapSym.get(z).map(w => {

                val ags = Seq(core.Ref(w), core.Lit(method.name.toString), core.Lit(method.name.toString), core.Lit(1))
                val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(), Seq(ags))
                core.ValDef(mapSym(x), rhs)
              })

            // translate case class constructors in return position
            case core.DefCall(_, app2, _, Seq(args))
              if x == r && isProductApply(x) =>

              val tgt1 = api.Sym[Seq.type].asModule
              val app1 = tgt1.info.member(api.TermName.app).asMethod

              val as1 = app.paramLists.head.map(p =>
                core.Lit(p.name.toString)
              )
              val as2 = args.map({
                case core.Ref(a) if mapSym contains a => core.Ref(mapSym(a))
                case a => a
              })
              //val Tpe = api.Type.kind1[Seq](api.Sym[ast.AttrRef].tpe)
              val lhs = mapSym(r)

              val argss = Seq(as2)
              val rhs = core.DefCall(Some(core.Ref(tgt1)), app1, Seq(api.Type[ast.AttrRef]), argss)
              Some(core.ValDef(lhs, rhs))

            case _ => None
          }

          val (vals2, expr1) = if (isProductApply(r)) {
            val vs2 = Seq.empty[Option[u.ValDef]]
            val ex2 = core.Ref(mapSym(r))

            (vs2, ex2)
          } else {
            val tgt = api.Sym[Seq.type].asModule
            val app = tgt.info.member(api.TermName.app).asMethod

            val as2 = Seq(core.Ref(mapSym(r)))

            val agss = Seq(as2)
            val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(api.Type[ast.AttrRef]), agss)
            val Tpe = api.Type.kind1[Seq](api.Type[ast.AttrRef])
            val lhs = api.TermSym.free(api.TermName.fresh("r"), Tpe)

            val vs2 = Seq(Some(core.ValDef(lhs, rhs)))
            val ex2 = core.Ref(lhs)

            (vs2, ex2)
          }


          //vals1.foreach(println)
          val ret = core.Lambda(Seq(mapSym(p)), core.Let((vals1 ++ vals2).flatten, Seq(), expr1))
          ret
        } else lambda

    }


  }

  private object JoinArgs {
    type Result = (u.TermSymbol, u.TermSymbol, u.TermSymbol, u.TermSymbol)

    def unapply(argss: Seq[Seq[u.Tree]]): Option[Result] = argss match {
      case Seq(Seq(core.Ref(kx), core.Ref(ky)), Seq(core.Ref(xs), core.Ref(ys))) => Some(kx, ky, xs, ys)
    }
  }

  private lazy val isNumeric = Set(
    api.Type.char,
    api.Type.byte,
    api.Type.short,
    api.Type.int,
    api.Type.long,
    api.Type.float,
    api.Type.double
  ) ++ Set(
    api.Type.Java.char,
    api.Type.Java.byte,
    api.Type.Java.short,
    api.Type.Java.int,
    api.Type.Java.long,
    api.Type.Java.float,
    api.Type.Java.double
  )

  private lazy val booleanOf = Map(
    //@formatter:off
    api.TermName("unary_!" ) -> ???,
    api.TermName("&&")       -> ast.And,
    api.TermName("||" )      -> ast.Or
    //@formatter:on
  )

  private lazy val comparisonOf = Map(
    //@formatter:off
    api.TermName(">" ) -> ast.GreaterThan,
    api.TermName(">=") -> ast.GreaterEqual,
    api.TermName("<" ) -> ast.LessThan,
    api.TermName("<=") -> ast.LessEqual,
    api.TermName("==") -> ast.Equal,
    api.TermName("eq") -> ast.Equal,
    api.TermName("!=") -> ast.Unequal,
    api.TermName("ne") -> ast.Unequal
    //@formatter:on
  )

  private lazy val arithmeticOf = Map(
    //@formatter:off
    api.TermName("+")  -> ???,
    api.TermName("-")  -> ???,
    api.TermName("*" ) -> ???,
    api.TermName("/")  -> ???,
    api.TermName("%")  -> ???
    //@formatter:on
  )

  private lazy val stringOf = Map(
    api.TermName("startsWith") -> ???
  )
}