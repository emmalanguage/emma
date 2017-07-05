#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.ml.clustering

import KMeans.Solution

import org.emmalanguage.test.util._
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.io.Source

import java.io.File

trait BaseKMeansIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/ml/clustering/kmeans"
  val path = tempPath(dir)
  val epsilon = 1e-3
  val iterations = 10
  val delimiter = "${symbol_escape}t"
  val overlap = .75

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    materializeResource(s"${symbol_dollar}dir/points.tsv")
    materializeResource(s"${symbol_dollar}dir/clusters.tsv")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  it should "cluster points around the corners of a hypercube" in {
    val exp =
      clusters(for {
        line <- Source.fromFile(s"${symbol_dollar}path/clusters.tsv").getLines().toSet[String]
      } yield {
        val Seq(point, cluster) = line.split(delimiter).map(_.toLong).toSeq
        point -> cluster
      })

    val act =
      clusters(for {
        s <- kMeans(exp.size, epsilon, iterations, s"${symbol_dollar}path/points.tsv")
      } yield (s.id, s.label.id))

    val correctClusters = for {
      act <- act
      exp <- exp
      if (act & exp).size / exp.size.toDouble >= overlap
    } yield ()

    correctClusters.size.toDouble should be >= (overlap * exp.size)
  }

  def clusters(associations: Set[(Long, Long)]): Iterable[Set[Long]] =
    for ((_, values) <- associations.groupBy { case (p, c) => c })
      yield values.map { case (p, c) => p }

  def kMeans(size: Int, epsilon: Double, iterations: Int, input: String): Set[Solution[Long]]
}
