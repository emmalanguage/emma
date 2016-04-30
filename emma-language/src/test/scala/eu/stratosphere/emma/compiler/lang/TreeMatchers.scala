package eu.stratosphere.emma.compiler.lang

import eu.stratosphere.emma.compiler.Compiler
import org.scalactic.{Bad, Good}
import org.scalatest.matchers.{BeMatcher, MatchResult}

trait TreeMatchers {

  val compiler: Compiler

  import compiler._
  import universe._

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

  def alphaEqTo(rhs: Tree) = new AlphaEqMatcher(rhs)
}
