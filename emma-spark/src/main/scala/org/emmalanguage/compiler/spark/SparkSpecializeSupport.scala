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
package compiler.spark

import compiler.SparkCompiler
import util.Monoids._

import shapeless._

import scala.collection.breakOut

private[compiler] trait SparkSpecializeSupport {
  self: SparkCompiler =>

  import API.DataBag
  import Core.{Lang => core}
  import SparkAPI.Ntv
  import SparkAPI.Ops
  import SparkAPI.Exp
  import UniverseImplicits._

  object SparkSpecializeSupport {
    /** Intorudces [[org.emmalanguage.api.spark.SparkNtv native Spark operators]] whenever possible. */
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
        .inherit { // accumulate lambdas in scope that can be specialized
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
              case core.DefCall(Some(xs), DataBag.withFilter, _, Seq(Seq(core.Ref(p))))
                if sp contains p =>
                val tgt = Ntv.ref
                val met = Ntv.select
                val tas = Seq(api.Type.arg(1, xs.tpe))
                val ass = Seq(Seq(mapFun(p)), Seq(xs))
                core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass))
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

    /** Specializes lambdas as [[org.emmalanguage.api.spark.SparkExp SparkExp]] expressions. */
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

        if (valOf.contains(r)) {
          val mapSym = Map(
            p -> api.TermSym.free(p.name, api.Type[String])
          ) ++ (vals.map(vd => {
            val x = vd.symbol.asTerm
            val W =
              if (x == r && isProductApply(x)) api.Type.kind1[Seq](Exp.Column)
              else Exp.Column
            val w = api.TermSym.free(x.name, W)
            x -> w
          })(breakOut): Map[u.TermSymbol, u.TermSymbol])

          val mapArgs = (args: Seq[u.Tree]) => args map {
            case core.Ref(z) if mapSym contains z => core.Ref(mapSym(z))
            case arg => arg
          }

          val vals1 = for (core.ValDef(x, rhs) <- vals) yield rhs match {
            // translate projections
            case core.DefCall(Some(core.Ref(z)), method, Seq(), Seq())
              if method.isGetter =>

              mapSym.get(z).map(w => {
                val tgt = Exp.ref
                val met = if (p == z) Exp.rootProj else Exp.nestProj
                val ags = Seq(core.Ref(w), core.Lit(method.name.toString))
                val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
                core.ValDef(mapSym(x), rhs)
              })

            // translate comparisons
            case core.DefCall(Some(y), method, _, zs)
              if isNumeric(y.tpe.widen) && (comparisonOf contains method.name) =>
              val tgt = Exp.ref
              val met = comparisonOf(method.name)
              val ags = mapArgs(y +: zs.flatten)
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
              Some(core.ValDef(mapSym(x), rhs))

            // translate boolean operators
            case core.DefCall(Some(y), method, _, zs)
              if y.tpe.widen =:= api.Type.bool && (booleanOf contains method.name) =>
              val tgt = Exp.ref
              val met = booleanOf(method.name)
              val ags = mapArgs(y +: zs.flatten)
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
              Some(core.ValDef(mapSym(x), rhs))

            // translate arithmetic operators
            case core.DefCall(Some(y), method, _, zs)
              if isNumeric(y.tpe.widen) && (arithmeticOf contains method.name) =>
              val tgt = Exp.ref
              val met = arithmeticOf(method.name)
              val ags = mapArgs(y +: zs.flatten)
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
              Some(core.ValDef(mapSym(x), rhs))

            // translate string operators
            case core.DefCall(Some(y), method, _, zs)
              if y.tpe.widen =:= api.Type.string && (stringOf contains method.name) =>
              val tgt = Exp.ref
              val met = stringOf(method.name)
              val ags = mapArgs(y +: zs.flatten)
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
              Some(core.ValDef(mapSym(x), rhs))

            // translate case class constructors in non-return position
            case core.DefCall(_, app, _, Seq(args))
              if x != r && isProductApply(x) =>
              val tgt = Exp.ref
              val met = Exp.nestStruct
              val as1 = app.paramLists.head.map(p =>
                core.Lit(p.name.toString)
              )
              val as2 = args.map({
                case core.Ref(a) if mapSym contains a => core.Ref(mapSym(a))
                case a => a
              })
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(as1, as2))
              Some(core.ValDef(mapSym(x), rhs))

            // translate case class constructors in return position
            case core.DefCall(_, app, _, Seq(args))
              if x == r && isProductApply(x) =>
              val tgt = Exp.ref
              val met = Exp.rootStruct
              val as1 = app.paramLists.head.map(p =>
                core.Lit(p.name.toString)
              )
              val as2 = args.map({
                case core.Ref(a) if mapSym contains a => core.Ref(mapSym(a))
                case a => a
              })
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(as1, as2))
              Some(core.ValDef(mapSym(x), rhs))

            case _ => None
          }

          val (vals2, expr1) = if (isProductApply(r)) {
            val vs2 = Seq.empty[Option[u.ValDef]]
            val ex2 = core.Ref(mapSym(r))

            (vs2, ex2)
          } else {
            val tgt = Exp.ref
            val met = Exp.rootStruct
            val as1 = Seq(core.Lit("_1"))
            val as2 = Seq(core.Ref(mapSym(r)))
            val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(as1, as2))
            val Tpe = api.Type.kind1[Seq](Exp.Column)
            val lhs = api.TermSym.free(api.TermName.fresh("r"), Tpe)

            val vs2 = Seq(Some(core.ValDef(lhs, rhs)))
            val ex2 = core.Ref(lhs)

            (vs2, ex2)
          }

          if (vals1.exists(_.isEmpty)) lambda // not all valdefs were translated
          else core.Lambda(Seq(mapSym(p)), core.Let((vals1 ++ vals2).flatten, Seq(), expr1))
        } else lambda

      case root =>
        root
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
    api.TermName("unary_!" ) -> Exp.not,
    api.TermName("&&")       -> Exp.and,
    api.TermName("||" )      -> Exp.or
    //@formatter:on
  )

  private lazy val comparisonOf = Map(
    //@formatter:off
    api.TermName(">" ) -> Exp.gt,
    api.TermName(">=") -> Exp.geq,
    api.TermName("<" ) -> Exp.lt,
    api.TermName("<=") -> Exp.leq,
    api.TermName("==") -> Exp.eq,
    api.TermName("eq") -> Exp.eq,
    api.TermName("!=") -> Exp.ne,
    api.TermName("ne") -> Exp.ne
    //@formatter:on
  )

  private lazy val arithmeticOf = Map(
    //@formatter:off
    api.TermName("+")  -> Exp.plus,
    api.TermName("-")  -> Exp.minus,
    api.TermName("*" ) -> Exp.multiply,
    api.TermName("/")  -> Exp.divide,
    api.TermName("%")  -> Exp.mod
    //@formatter:on
  )

  private lazy val stringOf = Map(
    api.TermName("startsWith") -> Exp.startsWith
  )
}
