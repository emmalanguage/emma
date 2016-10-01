/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
