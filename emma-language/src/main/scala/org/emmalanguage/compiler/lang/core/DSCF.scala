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
import compiler.ir.DSCFAnnotations._
import compiler.lang.cf.ControlFlow
import compiler.lang.source.Source
import util.Monoids._

import shapeless._

import scala.annotation.tailrec
import scala.collection.SortedSet
import scala.collection.breakOut

/** Direct-style control-flow transformation. */
private[core] trait DSCF extends Common {
  self: Core with Source with ControlFlow =>

  /**
   * Converts the control-flow in input tree in ANF in the direct-style.
   *
   * This eliminates `while` and `do-while` loops, `if-else` branches, and local variables,
   * and replaces them with nested, mutually recursive local methods and calls.
   *
   * For a description of the term `direct-style control flow` see Section 6.1.3 in [1].
   *
   * @see [[http://ssabook.gforge.inria.fr/latest/book.pdf Section 6.1.3 in the SSA Book]].
   *
   * == Examples ==
   * {{{
   *   { // if branch
   *     ..prefix
   *     ..condStats
   *     val cond = condExpr
   *     def suffix$1(res, ..vars) = { ... }
   *     def thn$1() = {
   *       ..thnStats
   *       suffix$1(thnExpr, ..vars)
   *     }
   *     def els$1() = {
   *       ..elsStats
   *       suffix$1(elsExpr, ..vars)
   *     }
   *     if (cond) thn$1() else els$1()
   *   }
   *
   *   { // while loop
   *     ..prefix
   *     def while$1(..vars) = {
   *       ..condStats
   *       val cond = condExpr
   *       def body$1() = {
   *         ..bodyStats
   *         while$1(..vars)
   *       }
   *       def suffix$2() = { ... }
   *       if (cond) body$1() else suffix$2()
   *     }
   *     while$1(..vars)
   *   }
   *
   *   { // do-while loop
   *     ..prefix
   *     def doWhile$1(..vars) = {
   *       ..bodyStats
   *       ..condStats
   *       val cond = condExpr
   *       def suffix$3() = { ... }
   *       if (cond) doWhile$1(..vars) else suffix$3()
   *     }
   *     doWhile$3(..vars)
   *   }
   * }}}
   */
  private[core] object DSCF {

    import ANF.AsBlock
    import API.DSCFAnnotations._
    import Core.{Lang => core}
    import Source.{Lang => src}
    import UniverseImplicits._

    private val noParams: Seq[Seq[u.TermSymbol]] = Seq(Seq.empty)
    private val noArgs:   Seq[Seq[u.Tree]]       = Seq(Seq.empty)

    /** Ordering symbols by their name. */
    implicit private val byName: Ordering[u.TermSymbol] =
      Ordering.by(_.name.toString)

    /** The Direct-Style Control-Flow (DSCF) transformation. */
    lazy val transform = TreeTransform("DSCF.transform", api.TopDown
      .withBindUses.withVarDefs.withOwnerChain
      .synthesize(Attr.collect[SortedSet, u.TermSymbol] {
        // Collect all variable assignments in a set sorted by name.
        case src.VarMut(lhs, _) => lhs
      }).accumulateWith[Map[(u.Symbol, u.TermName), u.Tree]] {
        // Accumulate the latest state of each variable per owner.
        case Attr(VarState(x, rhs), latest :: _, owners :: _, _) =>
          Map((owners.last, x.name) -> (rhs match {
            case src.VarRef(y) => currentState(y, latest, owners)
            case _ => rhs
          }))
        case Attr.none(core.DefDef(method, _, paramss, _)) =>
          paramss.flatten.map { case core.ParDef(p, _) =>
            (method, ParName.orig(p)) -> core.Ref(p)
          } (breakOut)
      } (overwrite).transformSyn {
        // Eliminate variables
        case Attr.none(src.VarDef(_, _)) => api.Empty()
        case Attr.none(src.VarMut(_, _)) => api.Empty()
        case Attr(src.VarRef(x), latest :: _, owners :: _, _) =>
          currentState(x, latest, owners)

        // Eliminate control-flow
        case Attr(block @ src.Block(stats, expr), _, (_ :+ owner) :: _, syn) =>
          // Extract required attributes
          val tpe = expr.tpe
          def uses(tree: u.Tree) = syn(tree).tail.tail.head
          def mods(tree: u.Tree) = syn(tree) match {
            case mods :: vars :: _ => mods diff vars.keySet
          }

          stats.span { // Split blocks
            case src.ValDef(_, src.Branch(_, _, _)) => false
            case src.Loop(_, _) => false
            case _ => true
          } match {
            // Linear
            case (_, Seq()) => block

            // { ..prefix; val x = if (cond) thn else els; ..suffix; expr }
            case (prefix, Seq(src.ValDef(lhs, src.Branch(cond, thn, els)), suffix@_*)) =>
              // Suffix
              val suffBody = src.Block(suffix, expr)
              val suffUses = uses(suffBody)
              val lhsFree  = suffUses(lhs) > 0
              val suffVars = ((mods(thn) | mods(els)) & suffUses.keySet).toSeq
              val suffArgs = varArgs(suffVars)
              val suffPars = if (lhsFree) lhs +: varPars(suffVars) else varPars(suffVars)
              val suffName = api.TermName.fresh("suffix")
              val suffMeth = monomorphic(owner, suffName, suffPars, tpe, suffixAnn)
              val suffCont = core.DefDef(suffMeth, paramss = Seq(suffPars), body = suffBody)

              def branchCont(name: u.TermName, body: u.Tree, ann: u.Annotation) = body match {
                case src.Block(branchStats, branchExpr) =>
                  val meth = monomorphic(owner, name, Seq.empty, tpe, ann)
                  val call = core.DefCall(None, meth, argss = noArgs)
                  val args = if (lhsFree) branchExpr +: suffArgs else suffArgs
                  val body = src.Block(branchStats, core.DefCall(None, suffMeth, argss = Seq(args)))
                  val cont = core.DefDef(meth, paramss = noParams, body = body)
                  (Some(cont), call)

                case _ =>
                  val argss = Seq(if (lhsFree) body +: suffArgs else suffArgs)
                  val call  = core.DefCall(None, suffMeth, argss = argss)
                  (None, call)
              }

              // Branches
              val (thnCont, thnCall) = branchCont(api.TermName.fresh("then"), thn, thenAnn)
              val (elsCont, elsCall) = branchCont(api.TermName.fresh("else"), els, elseAnn)
              src.Block(Seq.concat(prefix, Some(suffCont), thnCont, elsCont),
                expr = core.Branch(cond, thnCall, elsCall))

            // { ..prefix; while ({ ..condStats; condExpr }) { ..bodyStats; () }; ..suffix; expr }
            case (prefix, Seq(loop @ src.While(AsBlock(condStats, condExpr), AsBlock(bodyStats, _)), suffix@_*)) =>
              // Loop
              val loopVars = mods(loop).toSeq
              val loopPars = varPars(loopVars)
              val loopName = api.TermName.fresh("while")
              val loopMeth = monomorphic(owner, loopName, loopPars, tpe, whileAnn)
              val loopCall = core.DefCall(None, loopMeth, argss = Seq(varArgs(loopVars)))

              // Suffix
              val suffName = api.TermName.fresh("suffix")
              val suffMeth = monomorphic(loopMeth, suffName, Seq.empty, tpe, suffixAnn)

              // Loop body
              val bodyName = api.TermName.fresh("body")
              val bodyMeth = monomorphic(loopMeth, bodyName, Seq.empty, tpe, loopBodyAnn)

              src.Block(prefix :+
                core.DefDef(loopMeth, paramss = Seq(loopPars),
                  body = src.Block(condStats ++ Seq(
                    core.DefDef(bodyMeth, paramss = noParams,
                      body = src.Block(bodyStats, loopCall)),
                    core.DefDef(suffMeth, paramss = noParams,
                      body = src.Block(suffix, expr))),
                    core.Branch(condExpr,
                      thn = core.DefCall(None, bodyMeth, argss = noArgs),
                      els = core.DefCall(None, suffMeth, argss = noArgs)))),
                loopCall)

            // { ..prefix; do { ..bodyStats; () } while ({ ..condStats; condExpr }); ..suffix; expr }
            case (prefix, Seq(loop @ src.DoWhile(AsBlock(condStats, condExpr), AsBlock(bodyStats, _)), suffix@_*)) =>
              // Loop
              val loopVars = mods(loop).toSeq
              val loopPars = varPars(loopVars)
              val loopName = api.TermName.fresh("doWhile")
              val loopMeth = monomorphic(owner, loopName, loopPars, tpe, doWhileAnn)
              val loopCall = core.DefCall(None, loopMeth, argss = Seq(varArgs(loopVars)))

              // Suffix
              val suffName = api.TermName.fresh("suffix")
              val suffMeth = monomorphic(loopMeth, suffName, Seq.empty, tpe, suffixAnn)

              src.Block(prefix :+
                core.DefDef(loopMeth, paramss = Seq(loopPars),
                  body = src.Block(
                    stats = Seq.concat(bodyStats, condStats, Seq(
                      core.DefDef(suffMeth, paramss = noParams,
                        body = src.Block(suffix, expr)))),
                    expr = core.Branch(condExpr, loopCall,
                      els = core.DefCall(None, suffMeth, argss = noArgs)))),
                loopCall)
          }
    }._tree)

    /** The Direct-Style Control-Flow (DSCF) inverse transformation. */
    lazy val inverse = TreeTransform("DSCF.inverse", tree => {
      // construct dataflow graph
      val cfg = ControlFlow.cfg(tree)
      // construct transitive closure of nesting graph
      val nst = quiver.empty[u.Symbol, Unit, Unit]
        .addNodes(cfg.ctrl.nodes.map(quiver.LNode[u.Symbol, Unit](_, ())))
        .safeAddEdges(for {
          src <- cfg.ctrl.nodes
          dst <- api.Owner.chain(src)
          if dst != src
          if dst.isMethod
          if cfg.ctrl contains dst.asMethod
        } yield quiver.LEdge[u.Symbol, Unit](src, dst.asMethod, ()))
        .tclose.reverse

      // restrict dataflow to phi nodes only
      // and eliminate (x, y) edges if the owner of x contains the owner of y
      val phi = cfg.data.labfilter({
        case core.BindingDef(_, core.DefCall(_, API.GraphRepresentation.phi, _, _)) => true
        case _ => false
      }).efilter(e => {
        val srcOwn = e.from.owner
        val tgtOwn = e.to.owner
        !(nst.successors(srcOwn) contains tgtOwn)
      }).tclose

      // create a fresh variable `x` for each phi node `n` without non-nested successors
      // in the phi graph and associate `x` with `n` and each each predecessor `p` of `n`
      val varOf = (for {
        n <- phi.nodes
        if phi.outDegree(n) == 1
        x = api.Sym.With(n)(own = n.owner.owner, nme = api.TermName.fresh(n), flg = u.Flag.MUTABLE)
        p <- phi.predecessors(n)
      } yield p -> x) (breakOut): Map[u.Symbol, u.TermSymbol]

      def mkVarDefsWithNulls(pars: Seq[u.ValDef], owner: u.Symbol) = for {
        (core.ParDef(p, _), i) <- pars.zipWithIndex
        if phi.outDegree(p) == 1 // exclude `var x = null` defs if `x` has a successor in the phi graph
        bnddef <- {
          val (x1, rhs1) = (
            api.TermSym(owner, api.TermName.fresh("null"), p.info),
            nullAsInstanceOf(p.info)
          )
          val (x2, rhs2) = (
            varOf(p),
            src.Ref(x1)
          )
          Seq(src.ValDef(x1, rhs1), src.VarDef(x2, rhs2))
        }
      } yield bnddef

      def mkVarDefsWithDefaults(pars: Seq[u.ValDef], rhss: Seq[u.Tree]) = for {
        (core.ParDef(p, _), y) <- pars zip rhss
        if phi.outDegree(p) == 1 // exclude `var x = y` defs if `x` has a successor in the phi graph
      } yield src.VarDef(varOf(p), y)

      def mkCondValDefs(pars: Seq[u.ValDef], cnd: u.Tree, thns: Seq[u.Tree], elss: Seq[u.Tree]) = for {
        (core.ParDef(p, _), thn, els) <- (pars, thns, elss).zipped.toSeq
        if phi.outDegree(p) == 1 // exclude `var x = y` defs if `x` has a successor in the phi graph
        x = api.TermSym(p.owner.owner, api.TermName.fresh("if"), p.info)
        y = varOf(p)
        z <- Seq(
          src.ValDef(x, src.Branch(cnd, thn, els)),
          src.VarDef(y, core.Ref(x)))
      } yield z

      def mkMuts(lhss: Seq[u.Symbol], rhss: Seq[u.Tree]) = for {
        (x, y) <- lhss zip rhss
        if (y match { // exclude `x = y` assignments if `x` and `y` are mapped to the same variable
          case core.Ref(z) => varOf.getOrElse(x, x) != varOf.getOrElse(z, z)
          case _ => true
        })
      } yield src.VarMut(varOf(x), y)

      @tailrec
      def split(indx: Map[u.TermSymbol, u.Symbol])(body: u.Block, cond: u.Block): (u.Block, u.Block) = {
        val crefs = api.Tree.refs(cond) diff indx.keySet
        val body1 = src.Block(
          body.stats.filter({
            case src.ValDef(lhs, _) => !crefs(lhs)
            case _ => true
          }),
          body.expr
        )
        val cond1 = src.Block(
          body.stats.filter({
            case src.ValDef(lhs, _) => crefs(lhs)
            case _ => false
          }) ++ cond.stats,
          cond.expr
        )

        if (body1.stats.size != body.stats.size) split(indx)(body1, cond1)
        else (body, api.Tree.rename(indx.toSeq)(cond).asInstanceOf[u.Block])
      }

      @tailrec
      def prune(
        vals: Seq[u.ValDef], frontier: Set[u.TermSymbol]
      ): Seq[u.ValDef] = {
        val newFrontier = (for {
          core.BindingDef(lhs, rhs) <- vals
          if frontier contains lhs
          sym <- Set(lhs) ++ api.Tree.refs(rhs)
        } yield sym) (breakOut): Set[u.TermSymbol]

        val delta = newFrontier diff frontier
        if (delta.nonEmpty) prune(vals, newFrontier)
        else vals.filter(frontier contains _.symbol.asTerm)
      }

      api.BottomUp.unsafe.withOwner.transformWith {
        // a let block with nested `while($cond) { $body }` control-flow
        //@formatter:off
        case Attr.none(
          core.Let(
            pre1,
            Seq(
              core.DefDef(w, _, Seq(pars), core.Let(
                pre2,
                Seq(
                  core.DefDef(b, _, _,
                    src.Block(sbod, core.DefCall(None, m2, _, Seq(ags2)))),
                  core.DefDef(s, _, _,
                    suff: u.Block)),
                core.Branch(cond,
                  core.DefCall(None, m3, _, _),
                  core.DefCall(None, m4, _, _))
              ))),
            core.DefCall(None, m1, _, Seq(ags1)))
          ) if w == m1
            && w == m2
            && b == m3
            && s == m4
            && is[whileLoop](w)
            && is[loopBody](b)
            && is[suffix](s) =>
          //@formatter:on
          val vars = mkVarDefsWithDefaults(pars, ags1)
          val muts = mkMuts(pars.map(_.symbol), ags2)

          val con1 = {
            val pref = prune(pre2, api.Tree.refs(cond))
            val blck = src.Block(pref, cond)
            api.Tree.refresh(pref.map(_.symbol))(blck)
          }
          val bod1 = {
            val pref = prune(pre2, (sbod ++ muts).flatMap(api.Tree.refs).toSet)
            val blck = src.Block(pref ++ sbod ++ muts, api.Term.unit)
            api.Tree.refresh(pref.map(_.symbol))(blck)
          }
          val suf1 = {
            val pref = prune(pre2, api.Tree.refs(suff))
            val blck = src.Block(pref ++ suff.stats, suff.expr)
            api.Tree.refresh(pref.map(_.symbol))(blck).asInstanceOf[u.Block]
          }

          src.Block(
            Seq.concat(
              pre1,
              vars,
              Seq(
                src.While(
                  con1,
                  bod1)),
              suf1.stats
            ),
            suf1.expr)

        // a let block with nested `do { $body } while($cond)` control-flow
        //@formatter:off
        case Attr.none(
          core.Let(
            pref,
            Seq(
              core.DefDef(d, _, Seq(pars), core.Let(
                body,
                Seq(
                  core.DefDef(s, _, _,
                    suff: u.Block)),
                core.Branch(cond,
                  core.DefCall(None, m2, _, Seq(ags2)),
                  core.DefCall(None, m3, _, _))
              ))),
            core.DefCall(None, m1, _, Seq(ags1)))
          ) if d == m1
            && d == m2
            && s == m3
            && is[doWhileLoop](d)
            && is[suffix](s) =>
          //@formatter:on
          val indx = (for {
            (core.Ref(y), x) <- ags2 zip pars.map(_.symbol)
          } yield y -> varOf(x)) (breakOut): Map[u.TermSymbol, u.Symbol]

          val vars = mkVarDefsWithDefaults(pars, ags1)
          val muts = mkMuts(pars.map(_.symbol), ags2)
          val suf1 = api.Tree.rename(indx.toSeq)(suff).asInstanceOf[u.Block]
          val blc1 = src.Block(body ++ muts, cond)
          val (bod1, con1) = split(indx)(
            src.Block(blc1.stats, api.Term.unit),
            src.Block(Seq.empty, blc1.expr))

          src.Block(
            Seq.concat(
              pref,
              vars,
              Seq(src.DoWhile(con1, bod1)),
              suf1.stats
            ),
            suf1.expr)

        // a let block with nested `if($cond) suff($args1) else suff($args2)` control-flow
        //@formatter:off
        case Attr(
          core.Let(
            pref,
            Seq(
              core.DefDef(s, _, Seq(pars),
                suff: u.Block)),
            core.Branch(cond,
              core.DefCall(None, m1, _, Seq(argsthn)),
              core.DefCall(None, m2, _, Seq(argsels)))
          ), _, _ :: _, _)
            if s == m1
            && s == m2
            && is[continuation](s) =>
          //@formatter:off
          val vars = mkCondValDefs(pars, cond, argsthn, argsels)

          src.Block(
            Seq.concat(
              pref,
              vars,
              suff.stats
            ),
            suff.expr)

        // a let block with nested `if($cond) $thn` control-flow
        //@formatter:off
        case Attr(
          core.Let(
            pref,
            Seq(
              core.DefDef(s, _, Seq(pars),
                suff: u.Block),
              core.DefDef(t, _, _,
                src.Block(sthn, core.DefCall(None, m3, _, Seq(argsthn))))),
            core.Branch(cond,
              core.DefCall(None, m1, _, _),
              core.DefCall(None, m2, _, Seq(argsels)))
          ), _, owner :: _, _)
            if t == m1
            && s == m2
            && s == m3
            && is[thenBranch](t)
            && is[continuation](s) =>
          //@formatter:off
          val vars = mkVarDefsWithDefaults(pars, argsels)
          val thnb = src.Block(sthn ++ mkMuts(pars.map(_.symbol), argsthn), api.Term.unit)

          src.Block(
            Seq.concat(
              pref,
              vars,
              Seq(
                src.ValDef(
                  api.TermSym(owner, api.TermName.fresh("if"), api.Type.unit),
                  src.Branch(cond, thnb, api.Term.unit))),
              suff.stats
            ),
            suff.expr)

        // a let block with nested `if($cond) $els` control-flow
        //@formatter:off
        case Attr(
          core.Let(
            pref,
            Seq(
              core.DefDef(s, _, Seq(pars),
                suff: u.Block),
              core.DefDef(e, _, _,
                src.Block(sthn, core.DefCall(None, m3, _, Seq(argsels))))),
            core.Branch(cond,
              core.DefCall(None, m1, _, Seq(argsthn)),
              core.DefCall(None, m2, _, _))
          ), _, owner :: _, _)
            if s == m1
            && e == m2
            && s == m3
            && is[elseBranch](e)
            && is[continuation](s) =>
          //@formatter:off
          val vars = mkVarDefsWithDefaults(pars, argsthn)
          val elsb = src.Block(sthn ++ mkMuts(pars.map(_.symbol), argsels), api.Term.unit)

          src.Block(
            Seq.concat(
              pref,
              vars,
              Seq(
                src.ValDef(
                  api.TermSym(owner, api.TermName.fresh("if"), api.Type.unit),
                  src.Branch(cond, api.Term.unit, elsb))),
              suff.stats
            ),
            suff.expr)

        // a let block with nested `if($cond) $thn else $els` control-flow
        //@formatter:off
        case Attr(
          core.Let(
            pref,
            Seq(
              core.DefDef(s, _, Seq(pars),
                suff: u.Block),
              core.DefDef(t, _, _,
                src.Block(sthn, core.DefCall(None, m3, _, Seq(argsthn)))),
              core.DefDef(e, _, _,
                src.Block(sels, core.DefCall(None, m4, _, Seq(argsels))))),
            core.Branch(cond,
              core.DefCall(None, m1, _, _),
              core.DefCall(None, m2, _, _))
          ), _, owner :: _, _)
            if t == m1
            && e == m2
            && s == m3
            && s == m4
            && is[thenBranch](t)
            && is[elseBranch](e)
            && is[continuation](s) =>
          //@formatter:off
          val vars = mkVarDefsWithNulls(pars, owner)
          val thnb = src.Block(sthn ++ mkMuts(pars.map(_.symbol), argsthn), api.Term.unit)
          val elsb = src.Block(sels ++ mkMuts(pars.map(_.symbol), argsels), api.Term.unit)

          src.Block(
            Seq.concat(
              pref,
              vars,
              Seq(
                src.ValDef(
                  api.TermSym(owner, api.TermName.fresh("if"), api.Type.unit),
                  src.Branch(cond, thnb, elsb))),
              suff.stats
            ),
            suff.expr)

      }._tree.andThen(api.Tree.rename(varOf.toSeq))(tree)
    })

    /** Applies `f` to the deepest suffix (i.e. without control flow) in a let block. */
    def mapSuffix(let: u.Block, res: Option[u.Type] = None)
      (f: (Seq[u.ValDef], u.Tree) => u.Block): u.Block = (let, res) match {
        case (core.Let(vals, Seq(), expr), _) => f(vals, expr)
        case (_, None) =>
          api.BottomUp.break.withOwner.transformWith {
            case Attr.inh(core.Let(vals, Seq(), expr), api.DefSym(owner) :: _)
              if api.Sym.findAnn[suffix](owner).isDefined => f(vals, expr)
          }._tree(let).asInstanceOf[u.Block]
        case (_, Some(result)) =>
          val dict: Map[u.MethodSymbol, u.MethodSymbol] =
            api.Tree.methods(let).map { method =>
              val tps = method.typeParams.map(_.asType)
              val pss = method.paramLists.map(_.map(_.asTerm))
              val tpe  = api.Type.method(tps, pss, result)
              method -> api.Sym.With.tpe(method, tpe)
            } (breakOut)

          // Unsafe: The result type changes to Some(result).
          api.BottomUp.unsafe.withOwner.transformWith {
            case Attr.inh(core.Let(vals, defs, expr), owner :: _) =>
              if (defs.isEmpty && api.Sym.findAnn[suffix](owner).isDefined) f(vals, expr)
              else core.Let(vals, defs, expr)
            case Attr.none(core.Branch(cond, thn, els)) =>
              core.Branch(cond, thn, els)
            case Attr.none(core.DefCall(None, method, targs, argss))
              if dict.contains(method) =>
              core.DefCall(None, dict(method), targs, argss)
            case Attr.none(core.DefDef(method, tparams, paramss, body))
              if dict.contains(method) =>
              val pss = paramss.map(_.map(_.symbol.asTerm))
              core.DefDef(dict(method), tparams, pss, body)
          }._tree(let).asInstanceOf[u.Block]
      }

    /** Removes residual annotations from local methods. */
    lazy val stripAnnotations: u.Tree => u.Tree =
      tree => api.Tree.rename(api.Tree.defs(tree).map { m =>
        m -> api.Sym.With(m)(ans = Seq.empty)
      } (breakOut))(tree)

    // ------------------------
    // Helper methods & objects
    // ------------------------

    /** Creates a monomorphic method symbol (no type arguments, one parameter list). */
    private def monomorphic(own: u.Symbol, nme: u.TermName,
      pss: Seq[u.TermSymbol], res: u.Type, ann: u.Annotation
    ): u.MethodSymbol = api.DefSym(own, nme, Seq.empty, Seq(pss), res, ans = Seq(ann))

    /** Converts a seqeunce of variable symbols to an argumenet list. */
    private def varArgs(vars: Seq[u.TermSymbol]) =
      for (x <- vars) yield src.Ref(x)

    /** Converts a sequence of variable symbols to a parameter list. */
    private def varPars(vars: Seq[u.TermSymbol]) =
      for (x <- vars) yield api.ParSym(x.owner, ParName.from(x), x.info)

    /** Looks up the current state of a variable, given an environment and the owner chain. */
    private def currentState(x: u.TermSymbol,
      latest: Map[(u.Symbol, u.TermName), u.Tree],
      owners: Seq[u.Symbol]
    ) = owners.reverseIterator.map(_ -> x.name).collectFirst(latest).get

    /** Extractor for [[src.VarDef]] or [[src.VarMut]], i.e. a state change. */
    private object VarState {
      def unapply(tree: u.Tree): Option[(u.TermSymbol, u.Tree)] = tree match {
        case src.VarDef(x, rhs) => Some(x, rhs)
        case src.VarMut(x, rhs) => Some(x, rhs)
        case _ => None
      }
    }

    private def is[A: u.TypeTag](m: u.MethodSymbol): Boolean =
      api.Sym.findAnn[A](m).isDefined

    private def nullAsInstanceOf(tpe: u.Type): u.Tree =
      src.DefCall(Some(src.Lit(null)), _asInstanceOf, Seq(tpe))

    private lazy val _asInstanceOf =
      api.Type[Any].member(api.TermName("asInstanceOf")).asMethod

    object ParName {
      import api.TermName.regex

      private val suff = "$p"

      def from(x: u.Symbol): u.TermName = x.name match {
        case u.TermName(name) => api.TermName.fresh(name + suff)
      }

      def orig(x: u.Symbol): u.TermName = x.name match {
        case u.TermName(regex(name, _)) => u.TermName(name.stripSuffix(suff))
        case u.TermName(name) => u.TermName(name.stripSuffix(suff))
      }
    }

  }
}
