package eu.stratosphere.emma.compiler.lang.comprehension

import eu.stratosphere.emma.compiler.Common
import eu.stratosphere.emma.compiler.lang.core.Core

/** Resugarding and desugaring of comprehension syntax. */
private[comprehension] trait ReDeSugar extends Common {
  self: Core with Comprehension =>

  import universe._
  import Comprehension.{MonadOp, Syntax, asBlock}
  import Term._
  import Tree._

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
    def resugar(monad: Symbol)(tree: Tree): Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(monad: Symbol)

      object fn {
        val meta = new Core.Meta(tree)

        def unapply(tree: Tree): Option[(ValDef, Tree)] = tree match {
          case q"(${arg: ValDef}) => ${body: Tree}" =>
            Some(arg, body)
          case Term.ref(sym) =>
            meta.valdef(sym) map {
              case ValDef(_, _, _, Function(arg :: Nil, body)) =>
                (arg, body)
            }
        }
      }

      val transform: Tree => Tree = preWalk {
        case cs.map(xs, fn(arg, body)) =>
          cs.comprehension(
            List(
              cs.generator(Term sym arg, asBlock(xs))),
            cs.head(asBlock(body)))

        case cs.flatMap(xs, fn(arg, body)) =>
          cs.flatten(
            block(
              cs.comprehension(
                List(
                  cs.generator(Term sym arg, asBlock(xs))),
                cs.head(asBlock(body)))))

        case cs.withFilter(xs, fn(arg, body)) =>
          cs.comprehension(
            List(
              cs.generator(Term sym arg, asBlock(xs)),
              cs.guard(asBlock(body))),
            cs.head(block(ref(Term sym arg))))
      }

      (transform andThen Core.dce) (tree)
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
    def desugar(monad: Symbol)(tree: Tree): Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Syntax(monad: Symbol)

      def hasNestedBlocks(tree: Block): Boolean = {
        val inStats = tree.stats exists {
          case ValDef(_, _, _, _: Block) => true
          case _ => false
        }
        val inExpr = tree.expr match {
          case _: Block => true
          case _ => false
        }
        inStats || inExpr
      }

      val transform: Tree => Tree = postWalk {

        case parent: Block if hasNestedBlocks(parent) =>
          // flatten (potentially) nested block stats
          val flatStats = parent.stats.flatMap {
            case val_(sym, Block(stats, expr), flags) =>
              stats :+ val_(sym, expr, flags)
            case stat =>
              stat :: Nil
          }
          // flatten (potentially) nested block expr
          val flatExpr = parent.expr match {
            case Block(stats, expr) =>
              stats :+ expr
            case expr =>
              expr :: Nil
          }

          block(flatStats ++ flatExpr)

        case t@cs.comprehension(cs.generator(sym, block(_, rhs)) :: qs, cs.head(expr)) =>

          val (guards, rest) = qs span {
            case cs.guard(_) => true
            case _ => false
          }

          // accumulate filters in a prefix of valdefs and keep track of the tail valdef symbol
          val (tail, prefix) = {
            ({
              // step (1) accumulate the result
              (_: List[Tree]).foldLeft((Term sym rhs, List.empty[Tree]))((res, guard) => {
                val (currSym, currPfx) = res
                val cs.guard(expr) = guard

                val funcRhs = lambda(sym)(expr)
                val funcNme = Term.name.fresh("anonfun")
                val funcSym = Term.sym.free(funcNme, funcRhs.tpe)
                val funcVal = val_(funcSym, funcRhs)

                val nextNme = Term.name.fresh(cs.withFilter.symbol.name)
                val nextSym = Term.sym.free(nextNme, currSym.info)
                val nextVal = val_(nextSym, cs.withFilter(ref(currSym))(ref(funcSym)))

                (nextSym, nextVal :: funcVal :: currPfx)
              })
            } andThen {
              // step (2) reverse the prefix
              case (currSym, currPfx) => (currSym, currPfx.reverse)
            }) (guards)
          }

          expr match {
            case block(Nil, id: Ident) if id.symbol == sym =>
              // trivial head expression consisting of the matched sym 'x'
              // omit the resulting trivial mapper

              Core.simplify(block(
                prefix,
                ref(tail)))

            case _ =>
              // append a map or a flatMap to the result depending on
              // the size of the residual qualifier sequence 'qs'

              val (op: MonadOp, body: Tree) = rest match {
                case Nil => (
                  cs.map,
                  expr)
                case _ => (
                  cs.flatMap,
                  cs.comprehension(
                    rest,
                    cs.head(expr)))
              }

              val func = lambda(sym)(body)
              val term = Term.sym.free(Term.name.lambda, func.tpe)

              block(
                prefix,
                val_(term, func),
                op(ref(tail))(ref(term)))
          }

        case t@cs.flatten(block(stats, expr@cs.map(xs, fn))) =>

          block(
            stats,
            cs.flatMap(xs)(fn))
      }

      transform(tree)
    }
  }
}
