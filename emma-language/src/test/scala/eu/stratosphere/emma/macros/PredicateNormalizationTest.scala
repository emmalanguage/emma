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

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PredicateNormalizationTest extends FlatSpec with Matchers with RuntimeUtil {

  lazy val tb = mkToolBox()
  import universe._
  import syntax._

  val imports =
    q"import _root_.eu.stratosphere.emma.api._" ::
    q"import _root_.eu.stratosphere.emma.ir._" :: Nil


  "Predicate normalization" should "normalize predicates into CNF" in {
    val algorithm = q"""_root_.eu.stratosphere.emma.macros.parallelize {
      for {
        x <- DataBag(1 to 10)
        if !(x > 5 || (x < 2 && x == 0)) || (x > 5 || !(x < 2))
      } yield x
    }"""

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.<(2).`unary_!`.||(x.==(0).`unary_!`).||(x.>(5))))" :: Nil
  }

  it should "separate filter conjunction into seperate filters" in {
    val algorithm = q"""_root_.eu.stratosphere.emma.macros.parallelize {
      for {
        x <- DataBag(1 to 10)
        if x > 5 && x > 0 && x != 4
      } yield x
    }"""

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.!=(4)))" ::
      "(() => ((x: Int) => x.>(5)))" ::
      "(() => ((x: Int) => x.>(0)))" :: Nil
  }

  it should "separate filter conjunction and single predicate into separate filters" in {
    val algorithm = q"""_root_.eu.stratosphere.emma.macros.parallelize {
      for {
        x <- DataBag(1 to 10)
        if x > 5 && x > 0 && x != 4
        if x <= 2
      } yield x
    }"""

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.!=(4)))" ::
      "(() => ((x: Int) => x.>(5)))" ::
      "(() => ((x: Int) => x.>(0)))" ::
      "(() => ((x: Int) => x.<=(2)))" :: Nil
  }

  it should "seperate filter conjunction with UDF predicate into seperate filters" in {
    val algorithm = q"""_root_.eu.stratosphere.emma.macros.parallelize {
      def predicate1(x: Int) = x <= 2
      def predicate2(x: Int) = x > 5

      for {
        x <- DataBag(1 to 10)
        if predicate1(x) && x > 0 && x != 4
        if predicate2(x)
      } yield x
    }"""

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.!=(4)))" ::
      "((predicate2: Int => Boolean) => ((x: Int) => predicate2(x)))" ::
      "(() => ((x: Int) => x.>(0)))" ::
      "((predicate1: Int => Boolean) => ((x: Int) => predicate1(x)))" :: Nil
  }

  def filtersIn(algorithm: Tree): List[String] =
    q"{ ..$imports; $algorithm }".typeChecked collect {
      case q"$_.Filter.apply[$_](${Literal(Constant(f: String))}, ..$_)" => f
    }
}
