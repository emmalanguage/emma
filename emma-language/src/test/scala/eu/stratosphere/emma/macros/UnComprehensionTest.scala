package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api.DataBag

import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UnComprehensionTest extends FunSuite with Matchers with RuntimeUtil {

  lazy val tb = mkToolBox()
  import universe._

  val imports =
    q"import _root_.eu.stratosphere.emma.api._" ::
    q"import _root_.eu.stratosphere.emma.ir._" ::
    q"import _root_.eu.stratosphere.emma.runtime.Native" :: Nil

  test("fold-group fusion") {
    val actual = q"""_root_.eu.stratosphere.emma.macros.reComprehend {
      DataBag(1 to 100).groupBy { _ % 7 }.map { _.values.sum }.product
    }.run(Native())"""

    val expected = DataBag(1 to 100).groupBy { _ % 7 }.map { _.values.sum }.product
    actual should evalTo (expected)
  }

  test("filter only") {
    val actual = q"""_root_.eu.stratosphere.emma.macros.reComprehend {
      (for (x <- DataBag(1 to 100) if x % 2 == 0) yield x).sum
    }.run(Native())"""

    val expected = (for (x <- DataBag(1 to 100) if x % 2 == 0) yield x).sum
    actual should evalTo (expected)
  }

  test("simple flatMap") {
    val actual = q"""_root_.eu.stratosphere.emma.macros.reComprehend {
      (for {
        x <- DataBag(1 to 100)
        y <- DataBag(1 to 200)
        z = x + y
        if z % 2 == 0
      } yield z).sum
    }.run(Native())"""

    val expected = (for {
      x <- DataBag(1 to 100)
      y <- DataBag(1 to 200)
      z = x + y
      if z % 2 == 0
    } yield z).sum

    actual should evalTo (expected)
  }

  test("join") {
    val actual = q"""_root_.eu.stratosphere.emma.macros.reComprehend {
      (for {
        x <- DataBag(1 to 100)
        y <- DataBag(1 to 200)
        if x == y
      } yield x * y).sum
    }.run(Native())"""

    val expected = (for {
      x <- DataBag(1 to 100)
      y <- DataBag(1 to 200)
      if x == y
    } yield x * y).sum

    actual should evalTo (expected)
  }

  def evalTo[E](expected: E) =
    be (expected) compose { (actual: Tree) =>
      tb.eval(q"{ ..$imports; $actual }")
    }
}
