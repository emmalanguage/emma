package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common

import scala.annotation.tailrec

/** Common subexpression elimination (CSE) for the Core language. */
private[core] trait CSE extends Common {
  self: Core =>

  import universe._
  import Term._
  import Tree._

  private[core] object CSE {

    /**
     * Eliminates common subexpressions from a [[Tree]].
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - All common subexpressions and corresponding intermediate values are pruned.
     *
     * @param tree The [[Tree]] to be pruned.
     * @return A [[Tree]] with the same semantics but without common subexpressions.
     */
    def cse(tree: Tree): Tree = {
      type Dict = Map[Symbol, Tree]
      type Subst = (List[(TermSymbol, Tree)], Dict)

      @tailrec
      def loop(subst: Subst): Dict =
        subst match {
          case (Nil, aliases) => aliases
          case ((lhs1, rhs1) :: rest, aliases) =>
            val (eq, neq) = rest.partition { case (lhs2, rhs2) =>
              Type.of(lhs1) =:= Type.of(lhs2) &&
                rhs1.equalsStructure(rhs2)
            }

            val dict = {
              val dict = aliases ++ eq.map(_._1 -> Term.ref(lhs1))
              rhs1 match {
                case lit: Literal => dict + (lhs1 -> lit)
                case _ => dict
              }
            }

            val vals = neq.map { case (lhs, rhs) =>
              lhs -> Tree.subst(rhs, dict)
            }

            loop(vals, dict)
        }

      val vals = tree.collect {
        case value: ValDef if !Is.param(value) && value.rhs.nonEmpty =>
          // NOTE: Lazy vals not supported
          assert(!Is.lzy(value))
          Term.sym(value) -> value.rhs
      }

      val dict = loop((vals, Map.empty))
      expr(postWalk(tree) {
        case id: Ident if Has.termSym(id) && dict.contains(id.symbol) =>
          dict(id.symbol)

        case value: ValDef if dict contains value.symbol =>
          unit

        case Block(stats, expr) =>
          // Implicitly removes ()
          block(stats, expr)
      })
    }

    // Avoids blocks without statements
    private def expr(tree: Tree) = tree match {
      case Block(Nil, expr) => expr
      case _ => tree
    }
  }

}
