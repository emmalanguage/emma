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
  }
}
