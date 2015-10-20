package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api.DataBag
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UnComprehensionTest extends FunSuite with Matchers {

  lazy val cm = reflect.runtime.currentMirror
  lazy val tb = mkToolbox(s"-cp $toolboxClasspath")

  val imports = """
    |import eu.stratosphere.emma.api._
    |import eu.stratosphere.emma.ir._
    |import eu.stratosphere.emma.runtime.Native
    |""".stripMargin

  test("fold-group fusion") {
    val tree = tb parse imports + """
      |eu.stratosphere.emma.macros.testmacros.reComprehend {
      |  DataBag(1 to 100).groupBy { _ % 7 }.map { _.values.sum() }.product()
      |}.run(Native())
      |""".stripMargin

    val expected = DataBag(1 to 100).groupBy { _ % 7 }.map { _.values.sum() }.product()
    tb eval tree should be (expected)
  }

  test("filter only") {
    val tree = tb parse imports + """
      |eu.stratosphere.emma.macros.testmacros.reComprehend {
      |  (for (x <- DataBag(1 to 100) if x % 2 == 0) yield x).sum()
      |}.run(Native())
      |""".stripMargin

    val expected = (for (x <- DataBag(1 to 100) if x % 2 == 0) yield x).sum()
    tb eval tree should be (expected)
  }

  test("simple flatMap") {
    val tree = tb parse imports + """
      |eu.stratosphere.emma.macros.testmacros.reComprehend {
      |  (for {
      |    x <- DataBag(1 to 100)
      |    y <- DataBag(1 to 200)
      |    z = x + y
      |    if z % 2 == 0
      |  } yield z).sum()
      |}.run(Native())
      |""".stripMargin

    val expected = (for {
      x <- DataBag(1 to 100)
      y <- DataBag(1 to 200)
      z = x + y
      if z % 2 == 0
    } yield z).sum()

    tb eval tree should be (expected)
  }

  test("join") {
    val tree = tb parse imports + """
      |eu.stratosphere.emma.macros.testmacros.reComprehend {
      |  (for {
      |    x <- DataBag(1 to 100)
      |    y <- DataBag(1 to 200)
      |    if x == y
      |  } yield x * y).sum()
      |}.run(Native())
      |""".stripMargin

    val expected = (for {
      x <- DataBag(1 to 100)
      y <- DataBag(1 to 200)
      if x == y
    } yield x * y).sum()

    tb eval tree should be (expected)
  }
}
