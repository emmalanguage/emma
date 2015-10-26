package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.testutil._

import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class ConnectedComponentsTest extends FlatSpec with Matchers {
  import ConnectedComponents.Schema._

  "Connected components" should "find all connected sub-graphs" in withRuntime() { rt =>
    val rtName = sys.props.getOrElse("emma.execution.backend", "").toLowerCase
    if (rtName == "flink" || rtName == "native") {
      val actual = new ConnectedComponents("", "", rt).algorithm.run(rt)
      val expected =
        Component(1, 7) ::
        Component(7, 7) ::
        Component(2, 7) ::
        Component(6, 6) ::
        Component(4, 6) ::
        Component(3, 3) ::
        Component(5, 7) :: Nil

      compareBags(actual, expected)
    } else {
      println("""
        |Skipping Connected Components test, because it only works with Flink and Native.
        |(due to stateful API support)
        |""".stripMargin)
    }
  }
}
