package eu.stratosphere.emma.examples.graphs

import java.io.File

import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

import scala.io.Source

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class GraphColoringTest extends FlatSpec with PropertyChecks with Matchers with BeforeAndAfter {
  // default parameters
  val dir   = "/graphs/graph-col"
  val path  = tempPath(dir)

  before {
    new File(path).mkdirs()
    materializeResource(s"$dir/edges.tsv")
  }

  after {
    deleteRecursive(new File(path))
  }

  "Merging 2 lists of ranges" should "maintain order and compression" in {
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

  "After graph coloring any two neighbors" should "have different colors" in withRuntime() { rt =>
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
