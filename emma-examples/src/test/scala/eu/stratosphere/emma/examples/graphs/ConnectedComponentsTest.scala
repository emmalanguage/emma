package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.examples.graphs.ConnectedComponents.Schema.Component
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class ConnectedComponentsTest extends FunSuite with Matchers {

  val rt         =  runtime.default()

  test("ConnectedComponents calculates the connected components") {
    val result = new ConnectedComponents("","",rt).algorithm.run(rt)
    compareBags(result, Seq(Component(1,7), Component(7,7), Component(2,7), Component(6,6), Component(4,6), Component(3,3), Component(5,7)))
  }
}
