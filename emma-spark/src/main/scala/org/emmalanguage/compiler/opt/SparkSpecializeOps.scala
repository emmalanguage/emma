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
package compiler.opt

import compiler.SparkCompiler
import compiler.backend.BCtx

import scala.collection.breakOut

private[opt] trait SparkSpecializeOps {
  self: SparkCompiler =>

  import API.DataBag
  import Core.{Lang => core}
  import SparkAPI.Exp
  import SparkAPI.Ntv
  import SparkAPI.Ops
  import UniverseImplicits._

  private[opt] object SparkSpecializeOps {
    /** Introduces [[org.emmalanguage.api.spark.SparkNtv native Spark operators]] whenever possible. */
    lazy val specializeOps: TreeTransform = TreeTransform("SparkSpecializeOps.specializeOps", tree => {
      val cfg = ControlFlow.cfg(tree)
      val ctx = Context.bCtxMap(cfg)
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
        if ctx(lhs) == BCtx.Driver
        spec <- call match {
          // specialize `DataBag.withFilter` as `SparkNtv.select`
          case core.DefCall(Some(xs), DataBag.withFilter, _, Seq(Seq(core.Ref(p))))
            if specLambdas contains p =>
            val tgt = Ntv.ref
            val met = Ntv.select
            val tas = Seq(api.Type.arg(1, xs.tpe))
            val ass = Seq(Seq(specLambdaRef(p)), Seq(xs))
            Some(core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass)))
          // specialize `DataBag.map` as `SparkNtv.project`
          case core.DefCall(Some(xs), DataBag.map, Seq(t), Seq(Seq(core.Ref(f))))
            if supported(t) && (specLambdas contains f) =>
            val tgt = Ntv.ref
            val met = Ntv.project
            val tas = Seq(api.Type.arg(1, xs.tpe), t)
            val ass = Seq(Seq(specLambdaRef(f)), Seq(xs))
            Some(core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass)))
          // specialize `SparkOps.equiJoin` as `SparkNtv.equiJoin`
          case core.DefCall(_, Ops.equiJoin, tas, JoinArgs(kx, ky, xs, ys))
            if supported(tas) && (specLambdas contains kx) && (specLambdas contains ky) =>
            val tgt = Ntv.ref
            val met = Ntv.equiJoin
            val ass = Seq(Seq(specLambdaRef(kx), specLambdaRef(ky)), Seq(core.Ref(xs), core.Ref(ys)))
            Some(core.ValDef(lhs, core.DefCall(Some(tgt), met, tas, ass)))
          // specialize `SparkOps.cross` as `SparkNtv.cross`
          case core.DefCall(_, Ops.cross, tas, ass)
            if supported(tas) =>
            val tgt = Ntv.ref
            val met = Ntv.cross
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
      }(tree).tree
    })

    /** Specializes lambdas as [[org.emmalanguage.api.spark.SparkExp SparkExp]] expressions. */
    lazy val specializeLambda = TreeTransform("SparkspecializeLambda.specializeLambda", _ match {
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

        val nullComparisonOf = Map(
          //@formatter:off
          api.TermName("==") -> Exp.isNull,
          api.TermName("eq") -> Exp.isNull,
          api.TermName("!=") -> Exp.isNotNull,
          api.TermName("ne") -> Exp.isNotNull
          //@formatter:on
        )

        val eqComparisonOf = Map(
          //@formatter:off
          api.TermName("==") -> Exp.eq,
          api.TermName("eq") -> Exp.eq,
          api.TermName("!=") -> Exp.ne,
          api.TermName("ne") -> Exp.ne
          //@formatter:on
        )

        val ordComparisonOf = Map(
          //@formatter:off
          api.TermName(">" ) -> Exp.gt,
          api.TermName(">=") -> Exp.geq,
          api.TermName("<" ) -> Exp.lt,
          api.TermName("<=") -> Exp.leq
          //@formatter:on
        )

        val booleanOf = Map(
          //@formatter:off
          api.TermName("unary_!" ) -> Exp.not,
          api.TermName("&&")       -> Exp.and,
          api.TermName("||" )      -> Exp.or
          //@formatter:on
        )

        val arithmeticOf = Map(
          //@formatter:off
          api.TermName("+")  -> Exp.plus,
          api.TermName("-")  -> Exp.minus,
          api.TermName("*" ) -> Exp.multiply,
          api.TermName("/")  -> Exp.divide,
          api.TermName("%")  -> Exp.mod
          //@formatter:on
        )

        val stringOf = Map(
          api.TermName("startsWith") -> Exp.startsWith,
          api.TermName("contains") -> Exp.contains
        )

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

            // translate `null` comparisons
            case core.DefCall(Some(y), method, _, Seq(Seq(core.Lit(null))))
              if nullComparisonOf contains method.name =>
              val tgt = Exp.ref
              val met = nullComparisonOf(method.name)
              val ags = mapArgs(Seq(y))
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
              Some(core.ValDef(mapSym(x), rhs))

            // translate normal comparisons
            case core.DefCall(Some(y), method, _, zs)
              if eqComparisonOf contains method.name =>
              val tgt = Exp.ref
              val met = eqComparisonOf(method.name)
              val ags = mapArgs(y +: zs.flatten)
              val rhs = core.DefCall(Some(tgt), met, Seq(), Seq(ags))
              Some(core.ValDef(mapSym(x), rhs))

            // translate ordering comparisons
            case core.DefCall(Some(y), method, _, zs)
              if isNumeric(y.tpe.widen) && (ordComparisonOf contains method.name) =>
              val tgt = Exp.ref
              val met = ordComparisonOf(method.name)
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
    })

    private[opt] def supported(tpes: Seq[u.Type]): Boolean =
      tpes.forall(supported)

    private[opt] def supported(tpe: u.Type): Boolean =
      if (api.Type.isCaseClass(tpe)) {
        val pars = api.Type.caseClassParamsOf(tpe)
        val tpes = pars.values.toSeq
        supported(tpes)
      }
      else tpe.dealias.widen.typeConstructor match {
        case t if isSypportedTypeCtor(t.typeSymbol) =>
          supported(api.Type.arg(1, tpe))
        case t if isSupportedPrimType(t.typeSymbol) =>
          true
        case _ =>
          false
      }
  }

  private object JoinArgs {
    type Result = (u.TermSymbol, u.TermSymbol, u.TermSymbol, u.TermSymbol)

    def unapply(argss: Seq[Seq[u.Tree]]): Option[Result] = argss match {
      case Seq(Seq(core.Ref(kx), core.Ref(ky)), Seq(core.Ref(xs), core.Ref(ys))) => Some(kx, ky, xs, ys)
    }
  }

  private lazy val isSypportedTypeCtor = Set(
    api.Type.option.typeSymbol,
    api.Type.array.typeSymbol,
    api.Type.seq.typeSymbol
  )

  private lazy val isSupportedPrimType = Set(
    api.Type.bool.typeSymbol,
    api.Type.byte.typeSymbol,
    api.Type.short.typeSymbol,
    api.Type.int.typeSymbol,
    api.Type.long.typeSymbol,
    api.Type.float.typeSymbol,
    api.Type.double.typeSymbol,
    api.Type.string.typeSymbol,
    api.Type.bigInt.typeSymbol,
    api.Type.bigDec.typeSymbol,
    api.Type.Java.bool.typeSymbol,
    api.Type.Java.byte.typeSymbol,
    api.Type.Java.short.typeSymbol,
    api.Type.Java.int.typeSymbol,
    api.Type.Java.long.typeSymbol,
    api.Type.Java.float.typeSymbol,
    api.Type.Java.double.typeSymbol,
    api.Type.Java.string.typeSymbol,
    api.Type.Java.bigInt.typeSymbol,
    api.Type.Java.bigDec.typeSymbol
  )

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
}
