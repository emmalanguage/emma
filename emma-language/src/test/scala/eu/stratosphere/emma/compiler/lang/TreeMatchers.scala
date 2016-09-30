package eu.stratosphere.emma
package compiler
package lang

import org.scalactic._
import org.scalatest.matchers._

/** [[org.scalatest]] matchers for Scala ASTs. */
trait TreeMatchers {

  val compiler: Compiler

  import compiler._

  /** Test that the LHS tree is alpha-equivalent to the RHS tree. */
  def alphaEqTo[T <: u.Tree](rhs: T) =
    new AlphaEqMatcher(rhs)

  /** [[u.Tree]] matcher for alpha equivalence. */
  class AlphaEqMatcher(rhs: u.Tree) extends BeMatcher[u.Tree] {

    override def apply(lhs: u.Tree): MatchResult = {
      // Message helper function
      def msg(err: Neq, negated: Boolean) = s"""
         |Left tree ${if (negated) "was" else "was not"} alpha equivalent to right tree.
         |
         |${asSource("lhs")(lhs)}
         |${asSource("rhs")(rhs)}
         |
         |due to the following difference
         |
         |  "${err.msg}"
         |
         |detected at the following subtrees
         |
         |${asSource("lhs subtree")(err.lhs)}
         |${asSource("rhs subtree")(err.rhs)}
      """.stripMargin

      compiler.alphaEq(lhs, rhs) match {
        case Good(_) =>
          MatchResult(matches = true, "", "")
        case Bad(e) =>
          MatchResult(matches = false, msg(e, negated = false), msg(e, negated = true))
      }
    }
  }

  /** Matcher for accumulating [[org.scalactic.Or]] that prints all errors. */
  object good extends BeMatcher[Any Or Every[Any]] {
    override def apply(or: Any Or Every[Any]): MatchResult = or match {
      case Good(ok) =>
        MatchResult(matches = true, "", s"$ok was good")
      case Bad(errors) =>
        MatchResult(matches = false, errors.mkString("\n\n"), "")
    }
  }
}
