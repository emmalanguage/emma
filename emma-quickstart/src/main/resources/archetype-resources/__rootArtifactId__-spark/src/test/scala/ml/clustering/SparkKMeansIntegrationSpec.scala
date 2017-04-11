#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package ml.clustering

import KMeans.Solution
import ml.model._

import breeze.linalg.{Vector => Vec}
import org.emmalanguage.SparkAware
import org.emmalanguage.api.Meta.Projections._
import org.emmalanguage.api._
import org.emmalanguage.io.csv._

class SparkKMeansIntegrationSpec extends BaseKMeansIntegrationSpec with SparkAware {

  override def kMeans(k: Int, epsilon: Double, iterations: Int, input: String): Set[Solution[Long]] =
    emma.onSpark {
      // read the input
      val points = for (line <- DataBag.readCSV[String](input, CSV())) yield {
        val record = line.split("${symbol_escape}t")
        Point(record.head.toLong, Vec(record.tail.map(_.toDouble)))
      }
      // do the clustering
      val result = KMeans(k, epsilon, iterations)(points)
      // return the solution as a local set
      result.fetch().toSet[Solution[Long]]
    }
}
