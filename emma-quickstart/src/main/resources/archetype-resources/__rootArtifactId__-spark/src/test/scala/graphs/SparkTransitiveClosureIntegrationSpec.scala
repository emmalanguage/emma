#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package graphs

import model.Edge

import org.emmalanguage.SparkAware
import org.emmalanguage.api._
import org.emmalanguage.io.csv._

class SparkTransitiveClosureIntegrationSpec extends BaseTransitiveClosureIntegrationSpec with SparkAware {

  override def transitiveClosure(input: String, csv: CSV): Set[Edge[Long]] =
    emma.onSpark {
      // read in set of edges
      val edges = DataBag.readCSV[Edge[Long]](input, csv)
      // build the transitive closure
      val paths = TransitiveClosure(edges)
      // return the closure as local set
      paths.fetch().toSet
    }
}
