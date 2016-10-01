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
package eu.stratosphere
package emma.codegen.spark

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.Spark
import eu.stratosphere.emma.testutil._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._

@RunWith(classOf[JUnitRunner])
class SparkRuntimeCompatibilityTest extends FlatSpec with Matchers {

  private def engine =
    Spark.testing()

  "DataBag" should "be usable within a UDF closure" in withRuntime(engine) {
    case spark: Spark =>
      val ofInterest = DataBag(1 to 10).map(_ * 10)
      val actual = spark.sc.parallelize(1 to 100)
        .filter(x => ofInterest.exists (_ == x))

      val expected = 1 to 100 filter { x => ofInterest.exists(_ == x) }
      compareBags(expected, actual.collect())
  }

  it should "be usable as a broadcast variable" in withRuntime(engine) {
    case spark: Spark =>
      val ofInterest = DataBag(1 to 10).map(_ * 10)
      val broadcast = spark.sc.broadcast(ofInterest)
      val actual = spark.sc.parallelize(1 to 100)
        .filter(x => broadcast.value.exists(_ == x))

      val expected = 1 to 100 filter { x => ofInterest.exists(_ == x) }
      compareBags(expected, actual.collect())
  }
}
