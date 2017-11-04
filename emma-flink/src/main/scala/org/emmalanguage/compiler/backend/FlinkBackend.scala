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
package compiler.backend

import compiler.Common
import compiler.FlinkCompiler

import scala.collection.breakOut

/** Translating to dataflows. */
private[compiler] trait FlinkBackend extends Common {
  self: FlinkCompiler =>

  object FlinkBackend {

    import Core.{Lang => core}
    import FlinkAPI._
    import UniverseImplicits._

    /* Target objects specialization map. */
    private val T = Map(
      //@formatter:off
      API.DataBag$.sym    -> DataBag$.ref,
      API.MutableBag$.sym -> MutableBag$.ref,
      API.Ops.sym         -> Ops.ref
      //@formatter:on
    ): Map[u.TermSymbol, u.Tree]

    /* Methods specialization map. */
    private val M = (for {
      (srcAPI, tgtAPI) <- Seq(
        //@formatter:off
        API.DataBag$    -> DataBag$,
        API.MutableBag$ -> MutableBag$,
        API.Ops         -> Ops
        //@formatter:on
      )
      (srcOp, tgtOp) <- {
        val srcOps = srcAPI.ops.toSeq.sortBy(_.name.toString)
        val tgtOps = tgtAPI.ops.toSeq.sortBy(_.name.toString)
        srcOps zip tgtOps
      }
    } yield srcOp -> tgtOp) (breakOut): Map[u.MethodSymbol, u.MethodSymbol]

    /** Specialize backend for Flink. */
    val transform = TreeTransform("FlinkBackend.transform", tree => {
      val G = ControlFlow.cfg(tree)
      val C = Context.bCtxGraph(G)
      val V = G.data.labNodes.map(_.label)

      // 1) construct "lambdas that need to be adapted" -> "adapted lambdas" map
      val broadcastLambdas = (for {
        core.ValDef(f, lambda@core.Lambda(_, _, _)) <- V
        if C.label(f).contains(BCtx.Driver)
        if C.outEdges(f).forall(e => e.label && C.label(e.to).contains(BCtx.Driver))
        bs = for {
          x <- api.Tree.closure(lambda)
          if x.info.typeSymbol == API.DataBag.sym
        } yield x
        if bs.nonEmpty
      } yield {
        val tpe = api.Type.fun(Seq(RuntimeContext), f.info.widen.dealias)
        val nme = api.TermName.fresh(f)
        val lhs = api.TermSym(f.owner, nme, tpe)

        val ctx = api.TermSym.free(api.TermName.fresh("ctx"), RuntimeContext)

        val sub = for {
          x <- bs.toSeq.sortBy(_.name.toString)
        } yield x -> api.TermSym(lhs, api.TermName.fresh(x), x.info)

        val pre = for {
          (x, y) <- sub
        } yield {
          val A = api.Type.arg(1, x.info)

          val tgt = Some(Ntv.ref)
          val met = Ntv.bag
          val tas = Seq(A)
          val ass = Seq(Seq(core.Ref(x)), Seq(core.Ref(ctx)))
          val rhs = api.DefCall(tgt, met, tas, ass)

          core.ValDef(y, rhs)
        }

        val rhs = core.Lambda(Seq(ctx), core.Let(
          pre :+ api.Tree.rename(sub)(core.ValDef(f, lambda)).asInstanceOf[u.ValDef],
          Seq.empty,
          core.Ref(f)
        ))

        f -> core.ValDef(lhs, api.Owner.at(lhs)(rhs))
      }) (breakOut): Map[u.TermSymbol, u.ValDef]

      // 2) construct substitution sequences for call sites of the lambdas from (1)
      val broadcastOps = (for {
        core.ValDef(xs, rhs@core.DefCall(_, m, _, argss)) <- V
        lambdas = refsIn(argss).filter(broadcastLambdas.contains).flatMap(G.data.label)
        if lambdas.nonEmpty
        bs = {
          for {
            l <- lambdas.toSet[u.ValDef]
            x <- api.Tree.closure(l.rhs)
            if x.info.typeSymbol == API.DataBag.sym
          } yield x
        }.toSeq.sortBy(_.name.toString)
        if bs.nonEmpty
      } yield {
        val A = api.Type.arg(1, xs.info)

        val ys = bs.map(_ => api.TermSym.fresh(xs))

        val zs = core.ValDef(ys.head, rhs match {
          case cs.Map(core.Ref(us), core.Ref(f)) =>
            val A = api.Type.arg(1, us.info)
            val B = api.Type.arg(1, xs.info)
            api.DefCall(Some(Ntv.ref), Ntv.map, Seq(A, B), Seq(
              Seq(core.Ref(broadcastLambdas(f).symbol.asTerm)),
              Seq(core.Ref(us))))
          case cs.FlatMap(core.Ref(us), core.Ref(f)) =>
            val A = api.Type.arg(1, us.info)
            val B = api.Type.arg(1, xs.info)
            api.DefCall(Some(Ntv.ref), Ntv.flatMap, Seq(A, B), Seq(
              Seq(core.Ref(broadcastLambdas(f).symbol.asTerm)),
              Seq(core.Ref(us))))
          case cs.WithFilter(core.Ref(us), core.Ref(p)) =>
            val A = api.Type.arg(1, us.info)
            api.DefCall(Some(Ntv.ref), Ntv.filter, Seq(A), Seq(
              Seq(core.Ref(broadcastLambdas(p).symbol.asTerm)),
              Seq(core.Ref(us))))
          case _ => abort(
            s"""
              |Illegal reference of (distributed) DataBags
              |${bs.map(b => s" - $b").mkString("\n")}
              |in some lambda referenced in the following term:
              |<code>
              |$rhs
              |</code>
              |where the lambas are defined as follows.
              |${lambdas.map(l => s"<code>\n$l\n</code>").mkString("\n")}
              |Currently, broadcast variables in the Flink backend are supported only in
              |`map`, `flatMap`, and `filter` operators.
            """.stripMargin)
        })

        val ss = for {
          (y, u, b) <- (ys.tail :+ xs, ys, bs).zipped.toList
        } yield {
          val B = api.Type.arg(1, b.info)

          val tgt = Some(Ntv.ref)
          val met = Ntv.broadcast
          val tas = Seq(A, B)
          val ass = Seq(Seq(core.Ref(u), core.Ref(b)))
          val rhs = api.DefCall(tgt, met, tas, ass)

          core.ValDef(y, rhs)
        }

        xs -> (zs +: ss)
      }) (breakOut): Map[u.TermSymbol, Seq[u.ValDef]]

      api.BottomUp.transform({
        // specialize API calls
        case core.ValDef(lhs, core.DefCall(Some(core.Ref(t)), m, targs, argss))
          if C.label(lhs).contains(BCtx.Driver) && (T contains t) && (M contains m) =>
          core.ValDef(lhs, core.DefCall(Some(T(t)), M(m), targs, argss))

        // substitute broadcast lambdas
        case core.ValDef(lhs, _)
          if broadcastLambdas contains lhs =>
          broadcastLambdas(lhs)

        // substitute broadcast ops
        case core.Let(vals, defs, expr)
          if vals.exists(broadcastOps contains _.symbol.asTerm) =>
          core.Let(vals flatMap { case value@core.ValDef(lhs, _) =>
            broadcastOps.getOrElse(lhs, Seq(value))
          }, defs, expr)
      })._tree(tree)
    })

    private lazy val cs = Comprehension.Syntax(API.DataBag.sym)

    private def refsIn(argss: Seq[Seq[u.Tree]]): Seq[u.TermSymbol] =
      argss.flatten collect {
        case core.Ref(x) => x
      }
  }

}
