package eu.stratosphere.emma.examples.graphs

import java.io.File

import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class TriangleCountTest extends FlatSpec with Matchers {
  // default parameters
  val dir  = "/graphs/triangle-cnt"
  val path = tempPath(dir)
  val rt   = runtime.default()
  // initialize resources
  new File(path).mkdirs()
  materializeResource(s"$dir/edges.tsv")

  "Triangle Count" should "find the correct number of triangles in a graph" in {
    val ts = new TriangleCount(s"$path/edges.tsv", "", rt).algorithm run rt
    ts should be (3629)
  }
}
