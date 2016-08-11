package eu.stratosphere.emma
package compiler.lang.comprehension

import compiler.Common
import compiler.lang.core.Core
import util.Monoids

import shapeless._

/** Resugaring and desugaring of comprehension syntax. */
private[comprehension] trait ReDeSugar extends Common {
  self: Core with Comprehension =>

  import Monoids._
  import UniverseImplicits._
  import Comprehension.asLet
  import Core.{Lang => core}

  private[comprehension] object ReDeSugar {

    /**
     * Resugars monad ops into mock-comprehension syntax.
     *
     * == Preconditions ==
     *
     * - The input tree is in ANF.
     *
     * == Postconditions ==
     *
     * - A tree where monad operations are resugared into one-generator mock-comprehensions.
     *
     * @param monad The symbol of the monad to be resugared.
     * @return A resugaring transformation for that particular monad.
     */
    def resugar(monad: u.Symbol): u.Tree => u.Tree = {
      // Construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad)

      // Handle the case when a lambda is defined externally
      def lookup(f: u.TermSymbol, owner: u.Symbol,
        lambdas: Map[u.TermSymbol, (u.TermSymbol, u.Tree)]) = {

        lambdas.get(f).map { case (arg, body) =>
          val sym = api.Sym.copy(arg)(flags = u.NoFlags).asTerm
          sym -> api.Tree.rename(arg -> sym)(body)
        }.getOrElse {
          val nme = api.TermName.fresh("x")
          val tpe = api.Type.result(f.info)
          val arg = api.ValSym(owner, nme, tpe)
          val tgt = Some(core.ValRef(f))
          val app = api.Term.member(f, api.TermName.app).asMethod
          arg -> core.DefCall(tgt)(app)(Seq(core.ValRef(arg)))
        }
      }

      api.TopDown.withOwner
        // Accumulate a LHS -> (arg, body) Map from lambdas
        .accumulate(Attr.group {
          case core.ValDef(lhs, core.Lambda(_, Seq(core.ParDef(arg, _, _)), body), _) =>
            lhs -> (arg, body)
        })
        // Re-sugar comprehensions
        .transformWith {
          case Attr(cs.Map(xs, core.Ref(f)), lambdas :: _, owner :: _, _) =>
            val (sym, body) = lookup(f, owner, lambdas)
            cs.Comprehension(
              Seq(
                cs.Generator(sym, asLet(xs))),
              cs.Head(asLet(body)))

          case Attr(cs.FlatMap(xs, core.Ref(f)), lambdas :: _, owner :: _, _) =>
            val (sym, body) = lookup(f, owner, lambdas)
            cs.Flatten(
              core.Let()()(
                cs.Comprehension(
                  Seq(
                    cs.Generator(sym, asLet(xs))),
                  cs.Head(asLet(body)))))

          case Attr(cs.WithFilter(xs, core.Ref(p)), lambdas :: _, owner :: _, _) =>
            val (sym, body) = lookup(p, owner, lambdas)
            cs.Comprehension(
              Seq(
                cs.Generator(sym, asLet(xs)),
                cs.Guard(asLet(body))),
              cs.Head(core.Let()()(core.Ref(sym))))
        }.andThen(_.tree).andThen(Core.dce)
    }

    /**
     * Desugars mock-comprehension syntax into monad ops.
     *
     * == Preconditions ==
     *
     * - An ANF tree with mock-comprehensions.
     *
     * == Postconditions ==
     *
     * - A tree where mock-comprehensions are desugared into comprehension operator calls.
     *
     * @param monad The symbol of the monad syntax to be desugared.
     * @return A desugaring transformation for that particular monad.
     */
    def desugar(monad: u.Symbol): u.Tree => u.Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad)

      api.BottomUp.transform {
        case let @ core.Let(vals, defs, expr) =>
          // Un-nest potentially nested simple let blocks occurring on the RHS of parent vals
          val flatVals = vals.flatMap {
            // match `val x = { nestedVals; nestedExpr }`
            case core.ValDef(x, core.Let(nestedVals, Seq(), nestedExpr), xflags) => nestedExpr match {
              // nestedExpr is a simple identifier `y` contained in the nestedVals
              // rename the original `val y = ...` to `val x = ...`
              case core.Ref(y) if nestedVals.exists(_.symbol == y) =>
                val rm = api.Tree.rename(y -> x)
                nestedVals.map({
                  case core.ValDef(`y`, rhs, yflags) => core.ValDef(x, rm(rhs), yflags)
                  case other => rm(other).asInstanceOf[u.ValDef]
                })
              // nestedExpr is something else
              // emit `nestedVals; val x = nestedExpr`
              case _ =>
                nestedVals :+ core.ValDef(x, nestedExpr, xflags)
            }
            case value =>
              Seq(value)
          }

          // Un-nest potentially nested simple let blocks occurring in the let-expression
          val (exprVals, exprAtom) = expr match {
            case core.Let(nestedVals, Seq(), nestedExpr) =>
              (nestedVals, nestedExpr)
            case _ =>
              (Seq.empty, expr)
          }

          if (vals.size == flatVals.size + exprVals.size) let
          else core.Let(flatVals ++ exprVals: _*)(defs: _*)(exprAtom)

        case cs.Comprehension(Seq(cs.Generator(sym, core.Let(_, _, rhs)), qs@_*), cs.Head(expr)) =>
          val (guards, rest) = qs.span {
            case cs.Guard(_) => true
            case _ => false
          }

          // Accumulate filters in a prefix of values and keep track of the tail value symbol
          val (tail, prefix) = guards.foldLeft(api.TermSym.of(rhs), Vector.empty[u.ValDef]) {
            case ((currSym, currPfx), cs.Guard(p)) =>
              val currRef = core.Ref(currSym)
              val funcRhs = core.Lambda(sym)(p)
              val funcNme = api.TermName.fresh("guard")
              val funcSym = api.TermSym.free(funcNme, funcRhs.tpe)
              val funcRef = core.Ref(funcSym)
              val funcVal = api.ValDef(funcSym, funcRhs)
              val nextNme = api.TermName(cs.WithFilter.symbol)
              val nextSym = api.TermSym.free(nextNme, currSym.info)
              val nextVal = api.ValDef(nextSym, cs.WithFilter(currRef)(funcRef))
              (nextSym, currPfx :+ funcVal :+ nextVal)
          }

          val tailRef = core.Ref(tail)
          expr match {
            case core.Let(Seq(), Seq(), core.Ref(`sym`)) =>
              // Trivial head expression consisting of the matched sym 'x'
              // Omit the resulting trivial mapper
              core.Let(prefix: _*)()(tailRef)

            case _ =>
              // Append a map or a flatMap to the result depending on
              // the size of the residual qualifier sequence 'qs'
              val (op, body) =
                if (rest.isEmpty) (cs.Map, expr)
                else (cs.FlatMap, cs.Comprehension(rest, cs.Head(expr)))

              val func = core.Lambda(sym)(body)
              val term = api.TermSym.free(api.TermName.lambda, func.tpe)
              val call = op(tailRef)(core.Ref(term))
              core.Let(prefix :+ core.ValDef(term, func): _*)()(call)
          }

        case cs.Flatten(core.Let(vals, defs, cs.Map(xs, f))) =>
          core.Let(vals: _*)(defs: _*)(cs.FlatMap(xs)(f))
      }.andThen(_.tree)
    }
  }
}
