package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common
import compiler.lang.source.Source
import util.Monoids

import cats.std.all._
import shapeless._

import scala.collection.SortedSet

/** Direct-style control-flow transformation. */
private[core] trait DSCF extends Common {
  this: Source with Core =>

  import Monoids._

  import Core.{Lang => core}
  import Source.{Lang => src}
  import UniverseImplicits._
  import api.TermName.fresh
  import u.Flag._

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

    /** Ordering symbols by their name. */
    implicit private val byName: Ordering[u.TermSymbol] =
      Ordering.by(_.name.toString)

    /** Attribute that tracks the two latest values of a variable. */
    private type Trace = Map[(u.Symbol, u.TermName), List[u.TermSymbol]]

    /** The Direct-Style Control-Flow (DSCF) transformation. */
    lazy val transform: u.Tree => u.Tree = api.TopDown
      .withBindUses.withVarDefs.withOwnerChain
      // Collect all variable assignments in a set sorted by name.
      .synthesize(Attr.collect[SortedSet, u.TermSymbol] {
        case src.VarMut(lhs, _) => lhs
      })
      // Accumulate all parameters (to refresh them at the end).
      .accumulate { case core.DefDef(_, _, _, paramss, _) =>
        (for (core.ParDef(lhs, _, _) <- paramss.flatten) yield lhs).toVector
      }
      // Accumulate the two latest values of a variable in a map per owner.
      .accumulateWith[Trace] {
        case Attr.inh(src.VarDef(lhs, _, _), owners :: _) =>
          trace(lhs, owners)
        case Attr.inh(src.VarMut(lhs, _), owners :: _) =>
          trace(lhs, owners)
        case Attr.none(core.DefDef(method, _, _, paramss, _)) =>
          (for (core.ParDef(lhs, _, _) <- paramss.flatten)
            yield (method, lhs.name) -> List(lhs)).toMap
      } (Monoids.merge(Monoids.sliding(2)))
      .transformWithSyn {
        // Linear transformations
        case Attr(src.VarDef(lhs, rhs, _), trace :: _, owners :: _, _) =>
          core.ValDef(latest(lhs, owners, trace).head, rhs)
        case Attr(src.VarMut(lhs, rhs), trace :: _, owners :: _, _) =>
          val curr #:: prev #:: _ = latest(lhs, owners, trace)
          core.ValDef(curr, api.Tree.rename(lhs -> prev)(rhs))
        case Attr(src.VarRef(lhs), trace :: _, owners :: _, _) =>
          core.BindingRef(latest(lhs, owners, trace).head)

        // Control-flow elimination
        case Attr(block @ src.Block(stats, expr withType tpe), _, owners :: _, syn) =>
          // Extract required attributes
          val owner = owners.lastOption.getOrElse(get.enclosingOwner)
          def uses(tree: u.Tree) = syn(tree)(Nat._2)
          def mods(tree: u.Tree) = syn(tree) match { case ms :: ds :: _ => ms diff ds.keySet }

          stats.span { // Split blocks
            case src.ValDef(_, src.Branch(_, _, _), _) => false
            case src.Loop(_, _) => false
            case _ => true
          } match {
            // Linear
            case (_, Seq()) => block

            // Already normalized
            case (prefix, Seq(
              core.ValDef(x,
                branch @ core.Branch(_,
                  core.Atomic(_) | core.DefCall(_, _, _, _*),
                  core.Atomic(_) | core.DefCall(_, _, _, _*)), _),
              suffix@_*)) if (expr match {
                case core.ValRef(`x`) => true
                case _ => false
              }) && suffix.forall {
                case core.DefDef(_, _, _, _, _) => true
                case _ => false
              } => block

            // If branch
            case (prefix, Seq(src.ValDef(lhs, src.Branch(cond, thn, els), _), suffix@_*)) =>
              // Suffix
              val sufBody = src.Block(suffix: _*)(expr)
              val sufUses = uses(sufBody)
              val sufVars = (mods(thn) | mods(els)) & sufUses.keySet
              val sufArgs = varArgs(sufVars)
              val usesRes = sufUses(lhs) > 0
              val sufPars = varPars(sufVars) ++ (if (usesRes) Some(lhs) else None)
              val sufMeth = api.DefSym(owner, fresh("suffix"))()(sufPars)(tpe)

              def branchDefCall(name: u.TermName, body: u.Tree) = body match {
                case src.Block(branchStats, branchExpr) =>
                  val meth = api.DefSym(owner, name)()(Seq.empty)(tpe)
                  val call = core.DefCall()(meth)(Seq.empty)
                  val args = sufArgs ++ (if (usesRes) Some(branchExpr) else None)
                  val defn = core.DefDef(meth)()(Seq.empty)(
                    src.Block(branchStats: _*)(
                      core.DefCall()(sufMeth)(args)))
                  (Some(defn), call)

                case _ =>
                  val call = core.DefCall()(sufMeth)(sufArgs ++
                    (if (usesRes) Some(body) else None))
                  (None, call)
              }

              // Branches
              val (thnDefn, thnCall) = branchDefCall(fresh("then"), thn)
              val (elsDefn, elsCall) = branchDefCall(fresh("else"), els)

              src.Block(
                prefix ++ Seq(
                  thnDefn,
                  elsDefn,
                  Some(core.DefDef(sufMeth)()(sufPars)(sufBody))
                ).flatten: _*)(
                core.Branch(cond, thnCall, elsCall))

            // While loop
            case (prefix, Seq(loop @ src.While(cond, src.Block(bodyStats, _)), suffix@_*)) =>
              val (condStats, condExpr) = decompose(cond)

              // Loop
              val loopVars = mods(loop)
              val loopArgs = varArgs(loopVars)
              val loopPars = varPars(loopVars)
              val loopMeth = api.DefSym(owner, api.TermName.While())()(loopPars)(tpe)
              val loopCall = core.DefCall()(loopMeth)(loopArgs)

              // Suffix
              val sufBody = src.Block(suffix: _*)(expr)
              val sufVars = loopVars & uses(sufBody).keySet
              val sufArgs = if (sufVars.size == loopVars.size) loopArgs else varArgs(sufVars)
              val sufPars = if (sufVars.size == loopVars.size) loopPars else varPars(sufVars)
              val sufMeth = api.DefSym(loopMeth, fresh("suffix"))()(sufPars)(tpe)

              // Loop body
              val bodyVars = loopVars & uses(src.Block(bodyStats: _*)()).keySet
              val bodyArgs = if (bodyVars.size == loopVars.size) loopArgs else varArgs(bodyVars)
              val bodyPars = if (bodyVars.size == loopVars.size) loopPars else varPars(bodyVars)
              val bodyMeth = api.DefSym(loopMeth, fresh("body"))()(bodyPars)(tpe)

              src.Block(prefix :+
                core.DefDef(loopMeth)()(loopPars)(
                  src.Block(condStats ++ Seq(
                    core.DefDef(bodyMeth)()(bodyPars)(
                      src.Block(bodyStats: _*)(loopCall)),
                    core.DefDef(sufMeth)()(sufPars)(sufBody)): _*)(
                    core.Branch(condExpr,
                      core.DefCall()(bodyMeth)(bodyArgs),
                      core.DefCall()(sufMeth)(sufArgs)))): _*)(
                loopCall)

            // Do-while loop
            case (prefix, Seq(loop @ src.DoWhile(cond, src.Block(bodyStats, _)), suffix@_*)) =>
              val (condStats, condExpr) = decompose(cond)

              // Loop
              val loopVars = mods(loop)
              val loopArgs = varArgs(loopVars)
              val loopPars = varPars(loopVars)
              val loopMeth = api.DefSym(owner, api.TermName.DoWhile())()(loopPars)(tpe)
              val loopCall = core.DefCall()(loopMeth)(loopArgs)

              // Suffix
              val sufMeth = api.DefSym(loopMeth, fresh("suffix"))()(Seq.empty)(tpe)

              src.Block(prefix :+
                core.DefDef(loopMeth)()(loopPars)(
                  src.Block(bodyStats ++ condStats ++ Seq(
                    core.DefDef(sufMeth)()(Seq.empty)(
                      src.Block(suffix: _*)(expr))): _*)(
                    core.Branch(condExpr,
                      loopCall,
                      core.DefCall()(sufMeth)(Seq.empty)))): _*)(
                loopCall)
          }
      }.andThen { case Attr.acc(tree, _ :: params :: _) =>
        api.Tree.refresh(params: _*)(tree) // Refresh all DefDef parameters.
      }

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
      val owner = owners.lastOption.getOrElse(get.enclosingOwner)
      val name = api.TermName.fresh(variable)
      val value = api.ValSym(owner, name, variable.info)
      Map((owner, variable.name) -> List(value.asTerm))
    }

    /** Returns a stream of the latest values of a variable. */
    private def latest(variable: u.TermSymbol, owners: Seq[u.Symbol], trace: Trace) =
      (get.enclosingOwner +: owners).reverseIterator
        .flatMap(trace(_, variable.name).reverse).toStream

    /** Variables -> Parameters mapping. */
    private def varPars(vars: SortedSet[u.TermSymbol]): Seq[u.TermSymbol] =
      vars.toSeq.map(api.Sym.copy(_)(flags = PARAM).asTerm)

    /** Variables -> Arguments mapping. */
    private def varArgs(vars: SortedSet[u.TermSymbol]): Seq[u.Ident] =
      vars.toSeq.map(src.VarRef(_))
  }
}
