package eu.stratosphere
package emma.compiler
package lang

import org.scalactic._
import org.scalatest.matchers._

trait TreeMatchers {

  val compiler: Compiler

  import compiler._
  import universe._

  def alphaEqTo(rhs: Tree) = new AlphaEqMatcher(rhs)

  class AlphaEqMatcher(rhs: Tree) extends BeMatcher[Tree] {
    def apply(lhs: Tree) = {
      // message constructor function
      def msg(err: Neq, negated: Boolean) =
        s"""
           |Left tree ${if (negated) "was" else "was not"} alpha equivalent to right tree.
           |
           |${asSource("lhs")(lhs)}
           |${asSource("rhs")(lhs)}
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

  object good extends BeMatcher[Any Or Every[Any]] {
    override def apply(or: Any Or Every[Any]): MatchResult = or match {
      case Good(ok) =>
        MatchResult(matches = true, "", s"$ok was good")
      case Bad(errors) =>
        MatchResult(matches = false, errors mkString "\n\n", "")
    }
  }
}
