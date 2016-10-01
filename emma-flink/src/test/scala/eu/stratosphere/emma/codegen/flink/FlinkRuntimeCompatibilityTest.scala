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
package emma.codegen.flink

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.Flink
import eu.stratosphere.emma.testutil._

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class FlinkRuntimeCompatibilityTest extends FlatSpec with Matchers {

  private def engine =
    Flink.testing()

  "DataBag" should "be usable within a UDF closure" in withRuntime(engine) {
    case flink: Flink =>
      val ofInterest = DataBag(1 to 10).map(_ * 10)
      val actual = flink.env.fromCollection(1 to 100)
        .filter(x => ofInterest.exists(_ == x))

      val expected = 1 to 100 filter { x => ofInterest.exists(_ == x) }
      compareBags(expected, actual.collect())
  }

  it should "be usable as a broadcast variable" in withRuntime(engine) {
    case flink: Flink =>
      val ofInterest = DataBag(1 to 10).map(_ * 10)
      val broadcast = flink.env.fromCollection(ofInterest.fetch())
      val actual = flink.env.fromCollection(1 to 100)
        .filter(new RichFilterFunction[Int] {

          var ofInterest: DataBag[Int] = _

          override def open(conf: Configuration) =
            ofInterest = DataBag(getRuntimeContext.getBroadcastVariable("ofInterest").asScala)

          override def filter(x: Int) =
            ofInterest.exists(_ == x)

        }).withBroadcastSet(broadcast, "ofInterest")

      val expected = 1 to 100 filter { x => ofInterest.exists(_ == x) }
      compareBags(expected, actual.collect())
  }
}
