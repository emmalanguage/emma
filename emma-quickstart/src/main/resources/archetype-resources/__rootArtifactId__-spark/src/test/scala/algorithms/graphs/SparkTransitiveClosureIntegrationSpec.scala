#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.graphs

import model.Edge

import org.emmalanguage.SparkAware
import org.emmalanguage.api._

class SparkTransitiveClosureIntegrationSpec extends BaseTransitiveClosureIntegrationSpec with SparkAware {

  override def transitiveClosure(input: String, csv: CSV): Set[Edge[Long]] =
    withDefaultSparkSession(implicit spark => emma.onSpark {
      // read in set of edges
      val edges = DataBag.readCSV[Edge[Long]](input, csv)
      // build the transitive closure
      val paths = TransitiveClosure(edges)
      // return the closure as local set
      paths.collect().toSet
    })
}
