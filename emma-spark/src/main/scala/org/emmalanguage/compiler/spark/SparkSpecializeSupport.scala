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

import scala.collection.breakOut

private[compiler] trait SparkSpecializeSupport {
  self: SparkCompiler =>

  import API.DataBag
  import Core.{Lang => core}
  import SparkAPI.Exp
  import SparkAPI.Ntv
  import SparkAPI.Ops
  import UniverseImplicits._

  object SparkSpecializeSupport {
    /** Introduces [[org.emmalanguage.api.spark.SparkNtv native Spark operators]] whenever possible. */
    lazy val specializeOps: u.Tree => u.Tree = tree => {
      val disRslt = Backend.disambiguate(tree)
      val disTree = disRslt._1
      val dparCtx = disRslt._2.toSet[u.Symbol]

      val cfg = ControlFlow.cfg(disTree)
      val vds = cfg.data.labNodes.map(_.label)

      // specializable lambdas
      val specLambdas = (for {
        core.ValDef(x, f@core.Lambda(_, _, _)) <- vds
        h = specializeLambda(f)
        if h != f
        y = api.TermSym(x.owner, api.TermName.fresh(x.name), h.tpe)
      } yield x -> core.ValDef(y, h)) (breakOut): Map[u.TermSymbol, u.ValDef]

      val specLambdaRef = (f: u.TermSymbol) => core.Ref(specLambdas(f).symbol.asTerm)

      // specializable operator calls
      val specOps = (for {
        core.ValDef(lhs, call) <- vds
        // don't specialize calls in a data-parallel context
        if (dparCtx intersect api.Owner.chain(lhs).toSet).isEmpty
        spec <- call match {
          // specialize `DataBag.withFilter` as `SparkOps.Native.select`
          case core.DefCall(Some(xs), DataBag.withFilter, _, Seq(Seq(core.Ref(p))))
            if specLambdas contains p =>
            val tgt = Ntv.ref
            val met = Ntv.select
            val tas = Seq(api.Type.arg(1, xs.tpe))
            val ass = Seq(Seq(specLambdaRef(p)), Seq(xs))
            Some(core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass)))
          // specialize `DataBag.map` as `SparkOps.Native.project`
          case core.DefCall(Some(xs), DataBag.map, Seq(t), Seq(Seq(core.Ref(f))))
            if specLambdas contains f =>
            val tgt = Ntv.ref
            val met = Ntv.project
            val tas = Seq(api.Type.arg(1, xs.tpe), t)
            val ass = Seq(Seq(specLambdaRef(f)), Seq(xs))
            Some(core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass)))
          // specialize `SparkOps.equiJoin` as `SparkOps.Native.equiJoin`
          case core.DefCall(_, Ops.equiJoin, tas, JoinArgs(kx, ky, xs, ys))
            if (specLambdas contains kx) && (specLambdas contains ky) =>
            val tgt = Ntv.ref
            val met = Ntv.equiJoin
            val ass = Seq(Seq(specLambdaRef(kx), specLambdaRef(ky)), Seq(core.Ref(xs), core.Ref(ys)))
            Some(core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass)))
          case _ =>
            None
        }
      } yield lhs -> spec) (breakOut): Map[u.TermSymbol, u.ValDef]

      // a map from specializable lambdas to (#total, #specialized) uses
      val specLambdaUses = for ((x, _) <- specLambdas) yield {
        val uses = cfg.data.successors(x)
        x -> (uses.size, uses.count(specOps.contains))
      }

      api.BottomUp.transform {
        case core.Let(vals, defs, expr) =>
          core.Let(for {
            x@core.ValDef(lhs, _) <- vals
            y <- {
              if (specLambdas contains lhs) {
                // Case 1) definition of a specialized lambda
                val (totlUses, specUses) = specLambdaUses(lhs)
                if (totlUses > 0) {
                  if (specUses == 0) {
                    // Case 1.a) only original lambda needed
                    Seq(x)
                  } else if (specUses == totlUses) {
                    // Case 1.b) only specialized lambda needed
                    Seq(specLambdas(lhs))
                  } else {
                    // Case 1.c) both lambdas needed
                    Seq(specLambdas(lhs), x)
                  }
                } else {
                  // Case 1.d) not used at all
                  Seq.empty[u.ValDef]
                }
              } else if (specOps contains lhs) {
                // Case 2) definition of a specialized operator call
                Seq(specOps(lhs))
              } else {
                // Case 3) something else
                Seq(x)
              }
            }
          } yield y, defs, expr)
      }(disTree).tree
    }

    /** Specializes lambdas as [[org.emmalanguage.api.spark.SparkExp SparkExp]] expressions. */
    lazy val specializeLambda: u.Tree => u.Tree = {
      case lambda@core.Lambda(_, Seq(core.ParDef(p, _)), core.Let(vals, Seq(), core.Ref(r))) =>

        val valOf = vals.map(vd => {
          vd.symbol.asTerm -> vd.rhs
        })(breakOut): Map[u.TermSymbol, u.Tree]

        def isProductApply(x: u.TermSymbol): Boolean = valOf.get(x) match {
          case Some(core.DefCall(Some(core.Ref(t)), method, _, Seq(_))) if method.isSynthetic
            && method.name == api.TermName.app
            && t.companion.info.baseClasses.contains(api.Sym[Product]) => true
          case _ => false
        }

        if (p == r || valOf.contains(r)) {
          val mapSym = Map(
            p -> api.TermSym.free(p.name, Exp.Root)
          ) ++ (vals.map(vd => {
            val x = vd.symbol.asTerm
            x -> api.TermSym.free(x.name, Exp.Type)
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
                val met = Exp.proj
                val ags = Seq(core.Ref(w), core.Lit(method.name.toString))
                val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
                core.ValDef(mapSym(x), rhs)
              })

            // translate case class constructors
            case core.DefCall(_, app, _, Seq(args))
              if isProductApply(x) =>
              val tgt = Exp.ref
              val met = Exp.struct
              val as1 = app.paramLists.head.map(p =>
                core.Lit(p.name.toString)
              )
              val as2 = args.map({
                case core.Ref(a) if mapSym contains a => core.Ref(mapSym(a))
                case a => a
              })
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(as1, as2))
              Some(core.ValDef(mapSym(x), rhs))

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

            case _ => None
          }

          val expr1 = core.Ref(mapSym(r))

          if (vals1.exists(_.isEmpty)) lambda // not all valdefs were translated
          else core.Lambda(Seq(mapSym(p)), core.Let(vals1.flatten, Seq(), expr1))
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
