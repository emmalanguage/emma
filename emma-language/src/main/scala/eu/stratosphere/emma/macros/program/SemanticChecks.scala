package eu.stratosphere.emma.macros.program

import eu.stratosphere.emma.api.StatefulAccessedFromUdfException
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionAnalysis

private[emma] trait SemanticChecks extends ComprehensionAnalysis {
  import universe._
  import syntax._

  def doSemanticChecks(tree: Tree): Unit =
    checkForStatefulAccessedFromUdf(tree)

  /**
   * Checks that .bag() is not called from the UDF of an updateWith* on the same stateful that is being updated.
   */
  def checkForStatefulAccessedFromUdf(tree: Tree): Unit = traverse(tree) {
    case q"${updateWith @ Select(id: Ident, _)}[$_](${udf: Tree})"
      if api.updateWith.contains(updateWith.symbol) &&
        id.hasTerm && udf.closure(id.term) =>
          throw new StatefulAccessedFromUdfException()
  }
}
