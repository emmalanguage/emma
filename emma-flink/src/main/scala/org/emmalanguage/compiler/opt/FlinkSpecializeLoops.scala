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

import compiler.FlinkCompiler
import compiler.ir.DSCFAnnotations._

import shapeless._

import scala.annotation.tailrec
import scala.collection.breakOut

private[opt] trait FlinkSpecializeLoops {
  self: FlinkCompiler =>

  import API.DataBag
  import Core.{Lang => core}
  import FlinkAPI.Ntv
  import UniverseImplicits._

  private[opt] object FlinkSpecializeLoops {

    /**
     * Specializes `while` loops as a `FlinkNtv.iterate` calls whenever possible.
     *
     * Matches `while` loops which iterate either over an integer in the [0, N)
     * range or over a Range(0, N) object and update a single `DataBag`. The
     * body of the loop should consist excludively of `DataBag` API operators
     * (i.e., it  should correspond to a dataflow consisting only of sources
     * and transformations) and should not depend on the iterator variable.
     */
    lazy val specializeLoops = TreeTransform("FlinkSpecializeLoops.specializeLoops", tree => {
      val G = ControlFlow.cfg(tree)
      api.BottomUp.withValUses.transformWith({
        case Attr.syn(t@core.Let(
          vals1,
          Seq(WhileLoop(loop, i, xs, core.Let(
            Seq(
              core.ValDef(cond, rhs)),
            Seq(
              Body(body, core.Let(
                vals3,
                Seq(),
              WhileCall(m3, i2, xs2))),
              Suffix(suff, core.Let(
                vals4,
                defs4,
                expr4
              ))),
            core.Branch(
              core.Ref(c1),
            core.DefCall(_, mT, _, _),
            core.DefCall(_, mE, _, _))))),
          WhileCall(m1, i1, xs1)
        ), valUses :: _) if c1 == cond
          && m1 == loop
          && m3 == loop
          && mT == body
          && mE == suff
          && splitDataflow(vals3, api.Tree.refs(xs2).toSet[u.Symbol]).isDefined
          && is0untilN(vals1, rhs).isDefined =>

          val N = is0untilN(vals1, rhs).get

          val fTpe = api.Type.fun(Seq(xs.info), xs.info)
          val fSym = api.TermSym.free(api.TermName.fresh(api.TermName.lambda), fTpe)
          val fRhs = core.Lambda(Seq(xs), {
            val vals = splitDataflow(vals3, api.Tree.refs(xs2).toSet[u.Symbol]).get

            val pref = {
              val defs = vals3.flatMap(api.Tree.defs).toSet
              val refs = vals.flatMap(api.Tree.refs).toSet

              var S = Seq.empty[u.TermSymbol]
              var D = for {
                f <- refs diff defs
                if api.Sym.funs(f.info.typeSymbol) && G.data.contains(f)
              } yield f

              while (D.nonEmpty) {
                S = D.toSeq.sortBy(_.name.toString) ++ S
                D = for {
                  f <- D.flatMap(G.data.label).flatMap(api.Tree.closure)
                  if api.Sym.funs(f.info.typeSymbol)
                  if !(S contains f) && G.data.contains(f)
                } yield f
              }

              S.flatMap(G.data.label)
            }

            val body = core.Let(pref ++ vals, Seq(), xs2)
            api.Tree.refresh(pref.flatMap(api.Tree.defs))(body)
          })

          val lSym = api.TermSym.free(api.TermName.fresh(xs), xs.info)
          val lRhs = core.DefCall(
            Some(Ntv.ref), Ntv.iterate,
            Seq(api.Type.arg(1, lSym.info)),
            Seq(
              Seq(xs1),
              Seq(N, core.Ref(fSym))
            ))

          val blst = api.Tree.refs(i1) union api.Tree.refs(rhs)
          val wlst = api.Tree.refs(N)
          val pref = exclude(vals1, blst, wlst, valUses -- blst)
          val suff = vals4.map(api.Tree.rename(Seq(xs -> lSym))(_).asInstanceOf[u.ValDef])
          val defs = defs4.map(api.Tree.rename(Seq(xs -> lSym))(_).asInstanceOf[u.DefDef])
          val expr = api.Tree.rename(Seq(xs -> lSym))(expr4)

          val rslt = core.Let(Seq(
            pref,
            Seq(
              core.ValDef(fSym, fRhs),
              core.ValDef(lSym, lRhs)
            ),
            suff
          ).flatten,
            defs,
            expr
          )

          api.Owner.at(loop.owner)(rslt)
      })._tree(tree)
    })

    private type VMap = Map[u.Symbol, u.Tree]

    private lazy val Int = api.Type.int
    private lazy val Bag = DataBag.tpe

    private lazy val dataflowOps = Set(
      API.DataBag.monadOps,
      API.DataBag.nestOps,
      API.DataBag.boolAlgOps,
      API.DataBag.partOps
    ).flatten ++ Set(
      API.DataBag$.ops
    ).flatten ++ Set(
      Comprehension.Syntax(API.DataBag.sym).Comprehension.symbol
    )

    private object WhileLoop {
      def unapply(defn: u.DefDef): Option[(u.MethodSymbol, u.TermSymbol, u.TermSymbol, u.Tree)] =
        defn match {
          case api.DefDef(m, _, Seq(Seq(core.ParDef(x1, _), core.ParDef(x2, _))), body)
            if api.Sym.findAnn[whileLoop](m).isDefined =>
            (x1.info.widen.typeConstructor, x2.info.widen.typeConstructor) match {
              case (Int, Bag) => Some(m, x1, x2, body)
              case (Bag, Int) => Some(m, x2, x1, body)
              case _ => None
            }
          case _ => None
        }
    }

    private object WhileCall {
      def unapply(defn: u.Tree): Option[(u.MethodSymbol, u.Tree, u.Tree)] =
        defn match {
          case core.DefCall(_, m, Seq(), Seq(Seq(x1, x2))) =>
            (x1.tpe.widen.typeConstructor, x2.tpe.widen.typeConstructor) match {
              case (Int, Bag) => Some(m, x1, x2)
              case (Bag, Int) => Some(m, x2, x1)
              case _ => None
            }
          case _ => None
        }
    }

    private object Body {
      def unapply(defn: u.DefDef): Option[(u.MethodSymbol, u.Tree)] =
        defn match {
          case api.DefDef(m, _, Seq(Seq()), body)
            if api.Sym.findAnn[loopBody](m).isDefined => Some(m, body)
          case _ => None
        }
    }

    private object Suffix {
      def unapply(defn: u.DefDef): Option[(u.MethodSymbol, u.Tree)] =
        defn match {
          case api.DefDef(m, _, Seq(Seq()), body)
            if api.Sym.findAnn[suffix](m).isDefined => Some(m, body)
          case _ => None
        }
    }

    @tailrec
    private def exclude(
      vals: Seq[u.ValDef],
      blacklist: Set[u.TermSymbol],
      whitelist: Set[u.TermSymbol],
      valUses: Map[u.TermSymbol, Int]
    ): Seq[u.ValDef] = {
      val (inBlacklist, notInBlacklist) = vals.partition(x => {
        val s = x.symbol.asTerm
        !(whitelist contains s) && (blacklist contains s) && valUses.getOrElse(s, 0) <= 1
      })

      if (inBlacklist.isEmpty) notInBlacklist else {
        val valUsesDelta = inBlacklist.flatMap(_ collect {
          case api.Ref(x) => x -> 1
        }).groupBy(_._1).map({
          case (x, uses) => x -> uses.size
        })
        val valUsesNew = valUses.map({
          case (x, uses) =>
            if (whitelist contains x) x -> uses
            else x -> (uses - valUsesDelta.getOrElse(x, 0))
        })
        val blacklistNew = blacklist ++
          inBlacklist.map(_.symbol.asTerm) ++
          valUsesNew.filter({ case (_, uses) => uses < 1 }).keys
        exclude(
          notInBlacklist,
          blacklistNew,
          whitelist,
          valUsesNew
        )
      }
    }

    @tailrec
    private def splitDataflow(
      vals: Seq[u.ValDef], frontier: Set[u.Symbol]
    ): Option[Seq[u.ValDef]] = {
      val (inFrontier, notInFrontier) = vals.partition(
        frontier contains _.symbol.asTerm
      )
      val newFrontier = (for {
        core.ValDef(lhs, rhs) <- inFrontier
        sym <- rhs match {
          case core.DefCall(Some(xs), m, _, argss)
            if dataflowOps contains m =>
            Set(lhs) ++ api.Tree.refs(xs) ++ argss.flatten.flatMap(api.Tree.refs)
          case _ =>
            Set(lhs)
        }
      } yield sym) (breakOut): Set[u.Symbol]

      val delta = newFrontier diff frontier
      if (delta.nonEmpty) splitDataflow(vals, newFrontier)
      else notInFrontier match {
        case Seq(core.ValDef(_, core.DefCall(Some(it), m, _, _)))
          if m.name.toString == "next" && it.tpe.widen =:= api.Type.kind1[Iterator](Int) =>
          Some(inFrontier)
        case Seq(core.ValDef(_, core.DefCall(Some(i), m, _, Seq(Seq(core.Lit(1))))))
          if m.name.toString == "$plus" && i.tpe.widen =:= api.Type.int =>
          Some(inFrontier)
        case _ =>
          None
      }
    }

    def is0untilN(vals: Seq[u.ValDef], check: u.Tree): Option[u.Tree] = {
      implicit val map = vals.map(vd => vd.symbol -> vd.rhs)(breakOut): VMap
      check match {
        case core.DefCall(Some(core.Ref(it)), m, Seq(), Seq())
          if m.name.toString == "hasNext" && it.info.widen =:= api.Type.kind1[Iterator](Int) =>
          toIterator(it)
        case core.DefCall(Some(i), m, _, Seq(Seq(n)))
          if m.name.toString == "$less" && i.tpe.widen =:= api.Type.int =>
          Some(n)
        case _ =>
          None
      }
    }

    private def toIterator(v: u.TermSymbol)(implicit map: VMap): Option[u.Tree] =
      map.get(v).flatMap {
        case core.DefCall(Some(core.Ref(range)), m, Seq(), Seq())
          if m.name.toString == "toIterator" => until(range)
        case _ => None
      }

    private def until(v: u.TermSymbol)(implicit map: VMap): Option[u.Tree] =
      map.get(v).flatMap {
        case core.DefCall(Some(core.Ref(rInt)), m, Seq(), Seq(Seq(n)))
          if m.name.toString == "until" => richInt(rInt).map(_ => n)
        case _ => None
      }

    private def richInt(v: u.TermSymbol)(implicit map: VMap): Option[u.Tree] =
      map.get(v).flatMap {
        case core.DefCall(Some(core.Ref(api.Sym.predef)), m, Seq(), Seq(Seq(n@core.Lit(0))))
          if m.name.toString == "intWrapper" => Some(n)
        case _ => None
      }
  }
}
