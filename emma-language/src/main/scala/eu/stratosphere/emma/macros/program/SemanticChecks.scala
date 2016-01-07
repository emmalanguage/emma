package eu.stratosphere.emma.macros.program

import eu.stratosphere.emma.api.{UnstableApplicationToEnclosingScopeException, StatefulAccessedFromUdfException}
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionAnalysis

private[emma] trait SemanticChecks extends ComprehensionAnalysis {
  import universe._
  import syntax._

  val doSemanticChecks: Tree => Unit = { t =>
    checkForStatefulAccessedFromUdf(t)
    checkForUnstableApplicationsInEnclosingScope(t)
  }


  /**
    * Checks that `.bag()` is not called from the UDF of an `updateWith*` on the same stateful that
    * is being updated.
    */
  def checkForStatefulAccessedFromUdf(tree: Tree) = traverse(tree) {
    case q"${updateWith @ Select(id: Ident, _)}[..$_](...${args: List[List[Tree]]})"
      if api.updateWith.contains(updateWith.symbol) && id.hasTerm =>
        val UDFs = if (args.size > 1) args(1) else args.head
        if (UDFs exists { _.closure(id.term) })
          throw new StatefulAccessedFromUdfException()
  }

  /**
    * Prohibits applications to functions / methods in the enclosing object / closure.
    * This includes write references to mutable variables or properties (will
    * become applications after type checking due to UAP).
    *
    * @param tree
    */
  def checkForUnstableApplicationsInEnclosingScope(tree: Tree) = traverse(tree) {
    case a @ Apply(Select(This(TypeName(_)), TermName(id)),_)
      if !a.symbol.asTerm.isStable =>
        throw new UnstableApplicationToEnclosingScopeException(id)
  }
}
