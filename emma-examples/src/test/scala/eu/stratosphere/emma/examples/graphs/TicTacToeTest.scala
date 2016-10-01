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

import eu.stratosphere.emma.testutil.{ExampleTest, withRuntime}
import org.junit.experimental.categories.Category

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class TicTacToeTest extends FlatSpec with Matchers {
  import TicTacToe._

  "TicTacToe" should "compute the game-theoretical values" in withRuntime() { engine =>
    val result = new TicTacToe(engine).algorithm.run(engine).fetch().map { _.vc }
    // Could be compared to the native rt, but it is too slow, so we use a "checksum" instead
    result.collect { case u: Undefined => u } should be ('empty)
    result.collect { case Win(depth) => depth }.sum should equal (8697)
    result.collect { case Loss(depth) => depth }.sum should equal (4688)
    result.collect { case Count(cnt) => cnt }.sum should equal (3495)
  }
}
