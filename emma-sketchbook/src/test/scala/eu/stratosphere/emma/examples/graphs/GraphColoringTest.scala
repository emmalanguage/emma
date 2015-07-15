package eu.stratosphere.emma.examples.graphs

import java.io.File

import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class GraphColoringTest extends FunSuite with PropertyChecks with Matchers {
  // default parameters
  val dir   = "/graphs/graph-col"
  val path  = tempPath(dir)
  val rt    = runtime.default()
  // initialize resources
  new File(path).mkdirs()
  materializeResource(s"$dir/edges.tsv")

  test("Merging of 2 lists of ranges should maintain order and compression") {
    import Ordering.Implicits._
    import GraphColoring.Schema._

    forAll { (xss: List[(Int, Int)], yss: List[(Int, Int)]) =>
      val ranges1 = compress(xss.map(r => r min r.swap).sorted)
      val ranges2 = compress(yss.map(r => r min r.swap).sorted)
      val merged  = merge(ranges1, ranges2)
      val flat    = merged flatMap { case (x, y) => x :: y :: Nil }
      merged should equal (merged.sorted)
      flat   should equal (flat.sorted)
    }
  }

  test("After graph coloring any two neighbors should have different colors") {
    new GraphColoring(s"$path/edges.tsv", s"$path/vertex-colors.tsv", rt).run()

    val edges = for {
      line <- Source.fromFile(s"$path/edges.tsv").getLines()
      record = line split "\t" map { _.toLong }
    } yield record(0) -> record(1)

    val vertexColor = (for {
      line <- Source.fromFile(s"$path/vertex-colors.tsv").getLines()
      record = line split "\t" map { _.toLong }
    } yield record(0) -> record(1)).toMap

    for ((src, dst) <- edges) {
      vertexColor(src) should not be vertexColor(dst)
    }
  }
}
