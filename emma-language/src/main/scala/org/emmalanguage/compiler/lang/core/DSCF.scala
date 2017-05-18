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
import compiler.lang.source.Source
import util.Monoids._

import shapeless._

import scala.collection.SortedSet
import scala.collection.breakOut

/** Direct-style control-flow transformation. */
private[core] trait DSCF extends Common {
  self: Core with Source =>

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
    lazy val transform: u.Tree => u.Tree = api.TopDown
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
            (method, api.TermName.original(p.name)._1) -> core.Ref(p)
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
      }._tree

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

    // ---------------
    // Helper methods
    // ---------------

    /** Creates a monomorphic method symbol (no type arguments, one parameter list). */
    private def monomorphic(own: u.Symbol, nme: u.TermName,
      pss: Seq[u.TermSymbol], res: u.Type, ann: u.Annotation
    ): u.MethodSymbol = api.DefSym(own, nme, Seq.empty, Seq(pss), res, ans = Seq(ann))

    /** Converts a seqeunce of variable symbols to an argumenet list. */
    private def varArgs(vars: Seq[u.TermSymbol]) =
      for (x <- vars) yield src.Ref(x)

    /** Converts a sequence of variable symbols to a parameter list. */
    private def varPars(vars: Seq[u.TermSymbol]) =
      for (x <- vars) yield api.ParSym(x.owner, api.TermName.fresh(x), x.info)

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
  }
}
