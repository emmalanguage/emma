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
import compiler.lang.source.Source
import compiler.ir.DSCFAnnotations.suffix
import util.Monoids._

import cats.std.all._
import shapeless._

import scala.collection.breakOut
import scala.collection.SortedSet

/** Direct-style control-flow transformation. */
private[core] trait DSCF extends Common {
  this: Source with Core =>

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
   *     def suffix$1(..vars, res) = { .. }
   *     def thn$1(..vars) = {
   *       ..thnStats
   *       suffix$1(..vars, thnExpr)
   *     }
   *     def els$1(..vars) = {
   *       ..elsStats
   *       suffix$1(..vars, elsExpr)
   *     }
   *     if (cond) thn$1(..vars) else els$1(..vars)
   *   }
   *
   *   { // while loop
   *     ..prefix
   *     def while$1(..vars) = {
   *       ..condStats
   *       val cond = condExpr
   *       def body$1(..vars) = {
   *         ..body
   *         while$1(..vars)
   *       }
   *       def suffix$2(..vars) = { .. }
   *       if (cond) body$1(..vars) else suffix$2(..vars)
   *     }
   *     while$1(..vars)
   *   }
   *
   *   { // do-while loop
   *     ..prefix
   *     def doWhile$1(..vars) = {
   *       ..body
   *       ..condStats
   *       val cond = condExpr
   *       def suffix$3(..vars) = { .. }
   *       if (cond) doWhile$1(..vars) else suffix$3(..vars)
   *     }
   *     doWhile$3(..vars)
   *   }
   * }}}
   */
  private[core] object DSCF {

    import u.Flag._
    import _API_.DSCFAnnotations._
    import UniverseImplicits._
    import Core.{Lang => core}
    import Source.{Lang => src}

    /** Creates a monomorphic method symbol (no type arguments, one parameter list). */
    private def monomorphic(own: u.Symbol, nme: u.TermName,
      pss: Seq[u.TermSymbol], res: u.Type, ann: u.Annotation
    ): u.MethodSymbol = api.DefSym(own, nme, Seq.empty, Seq(pss), res, ans = Seq(ann))

    /** Ordering symbols by their name. */
    implicit private val byName: Ordering[u.TermSymbol] =
      Ordering.by(_.name.toString)

    /** Attribute that tracks the two latest values of a variable. */
    private type Trace = Map[(u.Symbol, u.TermName), List[u.TermSymbol]]

    /** The Direct-Style Control-Flow (DSCF) transformation. */
    lazy val transform: u.Tree => u.Tree = api.TopDown
      .withBindUses.withVarDefs.withOwnerChain
      .synthesize(Attr.collect[SortedSet, u.TermSymbol] {
        // Collect all variable assignments in a set sorted by name.
        case src.VarMut(lhs, _) => lhs
      }).accumulate { case core.DefDef(_, _, paramss, _) =>
        // Accumulate all parameters (to refresh them at the end).
        (for (core.ParDef(lhs, _) <- paramss.flatten) yield lhs).toVector
      }.accumulateWith[Trace] {
        // Accumulate the two latest values of a variable in a map per owner.
        case Attr.inh(src.VarDef(lhs, _), owners :: _) =>
          trace(lhs, owners)
        case Attr.inh(src.VarMut(lhs, _), owners :: _) =>
          trace(lhs, owners)
        case Attr.none(core.DefDef(method, _, paramss, _)) =>
          (for (core.ParDef(lhs, _) <- paramss.flatten)
            yield (method, lhs.name) -> List(lhs)).toMap
      } (merge(sliding(2))).transformSyn {
        // Linear transformations
        case Attr(src.VarDef(lhs, rhs), trace :: _, owners :: _, _) =>
          core.ValDef(latest(lhs, owners, trace).head, rhs)
        case Attr(src.VarMut(lhs, rhs), trace :: _, owners :: _, _) =>
          val curr #:: prev #:: _ = latest(lhs, owners, trace)
          core.ValDef(curr, api.Tree.rename(Seq(lhs -> prev))(rhs))
        case Attr(src.VarRef(lhs), trace :: _, owners :: _, _) =>
          core.BindingRef(latest(lhs, owners, trace).head)

        // Control-flow elimination
        case Attr(block @ src.Block(stats, expr), _, owners :: _, syn) =>
          // Extract required attributes
          val tpe = expr.tpe
          val owner = owners.lastOption.getOrElse(api.Owner.encl)
          def uses(tree: u.Tree) = syn(tree)(Nat._2)
          def mods(tree: u.Tree) = syn(tree) match { case ms :: ds :: _ => ms diff ds.keySet }

          stats.span { // Split blocks
            case src.ValDef(_, src.Branch(_, _, _)) => false
            case src.Loop(_, _) => false
            case _ => true
          } match {
            // Linear
            case (_, Seq()) => block

            // Already normalized
            case (_, Seq(
              core.ValDef(x, core.Branch(_,
                core.Atomic(_) | core.DefCall(_, _, _, _),
                core.Atomic(_) | core.DefCall(_, _, _, _))),
              suffix@_*)) if (expr match {
                case core.ValRef(`x`) => true
                case _ => false
              }) && suffix.forall {
                case core.DefDef(_, _, _, _) => true
                case _ => false
              } => block

            // If branch
            case (prefix, Seq(src.ValDef(lhs, src.Branch(cond, thn, els)), suffix@_*)) =>
              // Suffix
              val sufBody = src.Block(suffix, expr)
              val sufUses = uses(sufBody)
              val sufVars = (mods(thn) | mods(els)) & sufUses.keySet
              val sufArgs = varArgs(sufVars)
              val usesRes = sufUses(lhs) > 0
              val sufPars = varPars(sufVars) ++ (if (usesRes) Some(lhs) else None)
              val sufName = api.TermName.fresh("suffix")
              val sufMeth = monomorphic(owner, sufName, sufPars, tpe, suffixAnn)

              def branchDefCall(name: u.TermName, body: u.Tree, ann: u.Annotation) = body match {
                case src.Block(branchStats, branchExpr) =>
                  val meth = monomorphic(owner, name, Seq.empty, tpe, ann)
                  val call = core.DefCall(None, meth, argss = Seq(Seq.empty))
                  val args = sufArgs ++ (if (usesRes) Some(branchExpr) else None)
                  val defn = core.DefDef(meth, paramss = Seq(Seq.empty),
                    body = src.Block(branchStats, core.DefCall(None, sufMeth, argss = Seq(args))))
                  (Some(defn), call)

                case _ =>
                  val call = core.DefCall(None, sufMeth,
                    argss = Seq(sufArgs ++ (if (usesRes) Some(body) else None)))
                  (None, call)
              }

              // Branches
              val (thnDefn, thnCall) = branchDefCall(api.TermName.fresh("then"), thn, thenAnn)
              val (elsDefn, elsCall) = branchDefCall(api.TermName.fresh("else"), els, elseAnn)
              src.Block(prefix ++ Seq(thnDefn, elsDefn,
                Some(core.DefDef(sufMeth, paramss = Seq(sufPars), body = sufBody))
              ).flatten, core.Branch(cond, thnCall, elsCall))

            // While loop
            case (prefix, Seq(loop @ src.While(cond, src.Block(bodyStats, _)), suffix@_*)) =>
              val (condStats, condExpr) = decompose(cond)

              // Loop
              val loopVars = mods(loop)
              val loopArgs = varArgs(loopVars)
              val loopPars = varPars(loopVars)
              val loopName = api.TermName.fresh("while")
              val loopMeth = monomorphic(owner, loopName, loopPars, tpe, whileAnn)
              val loopCall = core.DefCall(None, loopMeth, argss = Seq(loopArgs))

              // Suffix
              val sufBody = src.Block(suffix, expr)
              val sufVars = loopVars & uses(sufBody).keySet
              val sufArgs = if (sufVars.size == loopVars.size) loopArgs else varArgs(sufVars)
              val sufPars = if (sufVars.size == loopVars.size) loopPars else varPars(sufVars)
              val sufName = api.TermName.fresh("suffix")
              val sufMeth = monomorphic(loopMeth, sufName, sufPars, tpe, suffixAnn)

              // Loop body
              val bodyVars = loopVars & uses(src.Block(bodyStats)).keySet
              val bodyArgs = if (bodyVars.size == loopVars.size) loopArgs else varArgs(bodyVars)
              val bodyPars = if (bodyVars.size == loopVars.size) loopPars else varPars(bodyVars)
              val bodyName = api.TermName.fresh("body")
              val bodyMeth = monomorphic(loopMeth, bodyName, bodyPars, tpe, loopBodyAnn)

              src.Block(prefix :+
                core.DefDef(loopMeth, paramss = Seq(loopPars),
                  body = src.Block(condStats ++ Seq(
                    core.DefDef(bodyMeth, paramss = Seq(bodyPars),
                      body = src.Block(bodyStats, loopCall)),
                    core.DefDef(sufMeth, paramss = Seq(sufPars), body = sufBody)),
                    core.Branch(condExpr,
                      core.DefCall(None, bodyMeth, argss = Seq(bodyArgs)),
                      core.DefCall(None, sufMeth, argss = Seq(sufArgs))))),
                loopCall)

            // Do-while loop
            case (prefix, Seq(loop @ src.DoWhile(cond, src.Block(bodyStats, _)), suffix@_*)) =>
              val (condStats, condExpr) = decompose(cond)

              // Loop
              val loopVars = mods(loop)
              val loopArgs = varArgs(loopVars)
              val loopPars = varPars(loopVars)
              val loopName = api.TermName.fresh("doWhile")
              val loopMeth = monomorphic(owner, loopName, loopPars, tpe, doWhileAnn)
              val loopCall = core.DefCall(None, loopMeth, argss = Seq(loopArgs))

              // Suffix
              val sufName = api.TermName.fresh("suffix")
              val sufMeth = monomorphic(loopMeth, sufName, Seq.empty, tpe, suffixAnn)

              src.Block(prefix :+
                core.DefDef(loopMeth, paramss = Seq(loopPars),
                  body = src.Block(bodyStats ++ condStats ++ Seq(
                    core.DefDef(sufMeth, paramss = Seq(Seq.empty),
                      body = src.Block(suffix, expr))),
                    core.Branch(condExpr, loopCall,
                      core.DefCall(None, sufMeth, argss = Seq(Seq.empty))))),
                loopCall)
          }
      }.andThen { case Attr.acc(tree, _ :: params :: _) =>
        api.Tree.refresh(params)(tree) // Refresh all DefDef parameters.
      }

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

          api.BottomUp.withOwner.transformWith {
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

    /** Decomposes a `tree` into statements and expression (if it's a block). */
    private def decompose(tree: u.Tree) = tree match {
      case src.Block(stats, expr) => (stats, expr)
      case _ => (Seq.empty, tree)
    }

    /** Creates a fresh symbol for the latest value of a variable. */
    private def trace(variable: u.TermSymbol, owners: Seq[u.Symbol]) = {
      val owner = owners.lastOption.getOrElse(api.Owner.encl)
      val name = api.TermName.fresh(variable)
      val value = api.ValSym(owner, name, variable.info)
      Map((owner, variable.name) -> List(value.asTerm))
    }

    /** Returns a stream of the latest values of a variable. */
    private def latest(variable: u.TermSymbol, owners: Seq[u.Symbol], trace: Trace) =
      (api.Owner.encl +: owners).reverseIterator
        .flatMap(trace(_, variable.name).reverse).toStream

    /** Variables -> Parameters mapping. */
    private def varPars(vars: SortedSet[u.TermSymbol]): Seq[u.TermSymbol] =
      vars.toSeq.map(api.Sym.With(_)(flg = PARAM).asTerm)

    /** Variables -> Arguments mapping. */
    private def varArgs(vars: SortedSet[u.TermSymbol]): Seq[u.Ident] =
      vars.toSeq.map(src.VarRef(_))
  }
}
