package eu.stratosphere.emma
package compiler.lang.comprehension

import compiler.Common
import compiler.lang.core.Core

import scala.collection.mutable.ListBuffer

/** Resugarding and desugaring of comprehension syntax. */
private[comprehension] trait ReDeSugar extends Common {
  self: Core with Comprehension =>

  import UniverseImplicits._
  import Comprehension.{MonadOp, asLet}
  import Core.{Lang => core}

  private[comprehension] object ReDeSugar {

    /**
     * Resugars monad ops into mock-comprehension syntax.
     *
     * == Preconditions ==
     *
     * - The input [[Tree]] is in ANF.
     *
     * == Postconditions ==
     *
     * - A [[Tree]] where monad operations are resugared into one-generator mock-comprehensions.
     *
     * @param monad The [[Symbol]] of the monad to be resugared.
     * @param tree  The [[Tree]] to be resugared.
     * @return The input [[Tree]] with resugared comprehensions.
     */
    def resugar(monad: u.Symbol)(tree: u.Tree): u.Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad)

      object Lookup {
        val meta = new Core.Meta(tree)

        def unapply(tree: u.Tree): Option[u.Tree] = tree match {
          case core.ValRef(sym) => meta.valdef(sym).map(_.rhs)
          case _ => None
        }
      }

      val transform = api.TopDown.transform {
        case cs.Map(xs, Lookup(core.Lambda(_, arg :: Nil, body))) =>
          // FIXME: is there a cheap way to avoid mutability?
          val sym = api.Sym.of(arg).asTerm // API: get term sym directly
          resetFlags(sym, u.Flag.PARAM)
          setFlags(sym, u.Flag.SYNTHETIC)

          cs.Comprehension(
            Seq(
              cs.Generator(sym, asLet(xs))),
            cs.Head(asLet(body)))

        case cs.FlatMap(xs, Lookup(core.Lambda(_, arg :: Nil, body))) =>
          // FIXME: is there a cheap way to avoid mutability?
          val sym = api.Sym.of(arg).asTerm // API: get term sym directly
          resetFlags(sym, u.Flag.PARAM)
          setFlags(sym, u.Flag.SYNTHETIC)

          cs.Flatten(
            core.Let()()(
              cs.Comprehension(
                Seq(
                  cs.Generator(sym, asLet(xs))),
                cs.Head(asLet(body)))))

        case cs.WithFilter(xs, Lookup(core.Lambda(_, arg :: Nil, body))) =>
          // FIXME: is there a cheap way to avoid mutability?
          val sym = api.Sym.of(arg).asTerm // API: get term sym directly
          resetFlags(sym, u.Flag.PARAM)
          setFlags(sym, u.Flag.SYNTHETIC)

          cs.Comprehension(
            Seq(
              cs.Generator(sym, asLet(xs)),
              cs.Guard(asLet(body))),
            cs.Head(core.Let()()(core.ValRef(sym))))
      }

      ({
        transform(_: u.Tree).tree
      } andThen {
        Core.dce
      }) (tree)
    }

    /**
     * Desugars mock-comprehension syntax into monad ops.
     *
     * == Preconditions ==
     *
     * - An ANF [[Tree]] with mock-comprehensions.
     *
     * == Postconditions ==
     *
     * - A [[Tree]] where mock-comprehensions are desugared into comprehension operator calls.
     *
     * @param monad The [[Symbol]] of the monad syntax to be desugared.
     * @param tree  The [[Tree]] to be desugared.
     * @return The input [[Tree]] with desugared comprehensions.
     */
    def desugar(monad: u.Symbol)(tree: u.Tree): u.Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad)

      val transform = api.BottomUp.transform {

        case core.Let(vals, defs, expr) =>
          val uvals = ListBuffer.empty[u.ValDef]

          // unnest potentially nested simple let blocks occurring on the rhs of parent vals
          vals foreach {
            case core.ValDef(sym, core.Let(nvals, Nil, nexpr), flags) =>
              uvals ++= nvals
              uvals += core.ValDef(sym, nexpr, flags)
            case val_ =>
              uvals += val_
          }

          // unnest potentially nested simple let blocks occurring on the parent expr
          val uexpr = expr match {
            case core.Let(nvals, Nil, nexpr) =>
              uvals ++= nvals
              nexpr
            case _ =>
              expr
          }

          core.Let(uvals.result():_*)(defs:_*)(uexpr)

        case t@cs.Comprehension(cs.Generator(sym, core.Let(_, _, rhs)) :: qs, cs.Head(expr)) =>

          val (guards, rest) = qs span {
            case cs.Guard(_) => true
            case _ => false
          }

          // accumulate filters in a prefix of valdefs and keep track of the tail valdef symbol
          val (tail, prefix) = {
            ({
              // step (1) accumulate the result
              (_: List[u.Tree]).foldLeft(((api.Sym of rhs).asTerm, List.empty[u.ValDef]))((res, guard) => { // API
                val (currSym, currPfx) = res
                val cs.Guard(expr) = guard

                val funcRhs = core.Lambda(sym)(expr)
                val funcNme = api.TermName.fresh("guard")
                val funcSym = api.TermSym.free(funcNme, funcRhs.tpe)
                val funcVal = api.ValDef(funcSym, funcRhs)

                val nextNme = api.TermName(cs.WithFilter.symbol.name)
                val nextSym = api.TermSym.free(nextNme, currSym.info)
                val nextVal = api.ValDef(nextSym, cs.WithFilter(core.BindingRef(currSym))(core.BindingRef(funcSym)))

                (nextSym, nextVal :: funcVal :: currPfx)
              })
            } andThen {
              // step (2) reverse the prefix
              case (currSym, currPfx) => (currSym, currPfx.reverse)
            }) (guards)
          }

          expr match {
            case core.Let(Nil, Nil, core.BindingRef(`sym`)) =>
              // trivial head expression consisting of the matched sym 'x'
              // omit the resulting trivial mapper

              Core.simplify(core.Let(prefix: _*)()(core.BindingRef(tail)))

            case _ =>
              // append a map or a flatMap to the result depending on
              // the size of the residual qualifier sequence 'qs'

              val (op: MonadOp, body: u.Tree) = rest match {
                case Nil => (
                  cs.Map,
                  expr)
                case _ => (
                  cs.FlatMap,
                  cs.Comprehension(
                    rest,
                    cs.Head(expr)))
              }

              val func = core.Lambda(sym)(body)
              val term = api.TermSym.free(api.TermName.lambda, func.tpe)

              core.Let(
                prefix :+ core.ValDef(term, func):_*
              )()(op(core.BindingRef(tail))(core.BindingRef(term)))
          }

        case t@cs.Flatten(core.Let(vals, defs, expr@cs.Map(xs, fn))) =>
          core.Let(vals:_*)(defs:_*)(cs.FlatMap(xs)(fn))
      }

      transform(tree).tree
    }
  }

}
