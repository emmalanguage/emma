#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package graphs

import graphs.model.Edge

import org.emmalanguage.io.csv.CSV
import org.emmalanguage.test.util._
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.io.Source

import java.io.File

trait BaseTransitiveClosureIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/graphs/trans-closure"
  val path = tempPath(dir)

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    materializeResource(s"${symbol_dollar}dir/edges.tsv")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  "TransitiveClosure" should "compute the transitive closure of a directed graph" in {
    val graph = (for {
      line <- Source.fromFile(s"${symbol_dollar}path/edges.tsv").getLines
    } yield {
      val record = line.split('${symbol_escape}t').map(_.toLong)
      Edge(record(0), record(1))
    }).toSet

    val closure = transitiveClosure(s"${symbol_dollar}path/edges.tsv", CSV())

    graph subsetOf closure should be(true)
  }

  def transitiveClosure(input: String, csv: CSV): Set[Edge[Long]]
}
