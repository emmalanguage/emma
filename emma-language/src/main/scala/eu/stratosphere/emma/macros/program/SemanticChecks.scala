package eu.stratosphere.emma.macros.program

import eu.stratosphere.emma.api.StatefulAccessedFromUdfException
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionAnalysis

private[emma] trait SemanticChecks extends ComprehensionAnalysis {
  import universe._

  def doSemanticChecks(t: Tree): Unit =
    checkForStatefulAccessedFromUdf(t)

  /**
   * Checks that .bag() is not called from the UDF of an updateWith* on the same stateful that is being updated.
   */
  def checkForStatefulAccessedFromUdf(t: Tree): Unit =
    t.traverse {
      case q"${updateWith @ Select(ident @ Ident(_), _)}[$_]($udf)"
        if api.updateWith contains updateWith.symbol =>
        if ((udf: Tree).freeTerms.map(x => x.asInstanceOf[Symbol]) contains ident.symbol)
          throw new StatefulAccessedFromUdfException()
    }
}
