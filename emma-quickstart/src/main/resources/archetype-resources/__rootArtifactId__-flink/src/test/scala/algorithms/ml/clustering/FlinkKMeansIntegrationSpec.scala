#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.ml.clustering

import KMeans.Solution
import algorithms.ml.model._

import org.emmalanguage.FlinkAware
import org.emmalanguage.api._

class FlinkKMeansIntegrationSpec extends BaseKMeansIntegrationSpec with FlinkAware {

  override def kMeans(k: Int, epsilon: Double, iterations: Int, input: String): Set[Solution[Long]] =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      // read the input
      val points = for (line <- DataBag.readText(input)) yield {
        val record = line.split("${symbol_escape}t")
        Point(record.head.toLong, record.tail.map(_.toDouble))
      }
      // do the clustering
      val result = KMeans(2, k, epsilon, iterations)(points)
      // return the solution as a local set
      result.collect().toSet[Solution[Long]]
    })
}
