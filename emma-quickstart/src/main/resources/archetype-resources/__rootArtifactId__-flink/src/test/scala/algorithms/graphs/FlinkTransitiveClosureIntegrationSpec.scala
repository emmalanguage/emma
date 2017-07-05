#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.graphs

import model._

import org.emmalanguage.FlinkAware
import org.emmalanguage.api._

class FlinkTransitiveClosureIntegrationSpec extends BaseTransitiveClosureIntegrationSpec with FlinkAware {

  override def transitiveClosure(input: String, csv: CSV): Set[Edge[Long]] =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      // read in set of edges
      val edges = DataBag.readCSV[Edge[Long]](input, csv)
      // build the transitive closure
      val paths = TransitiveClosure(edges)
      // return the closure as local set
      paths.collect().toSet
    })
}
