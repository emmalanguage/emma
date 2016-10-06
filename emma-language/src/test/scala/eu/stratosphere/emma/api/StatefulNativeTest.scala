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
package eu.stratosphere.emma.api

import eu.stratosphere.emma.api.model.{Identity, id}

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatefulNativeTest extends FlatSpec with Matchers {

  "Stateful native" should "do deep copies" in  {
    import StatefulNativeTest._

    val initial = DataBag(Foo(1, 1) :: Nil)
    val withState = stateful[Foo, Int](initial)
    withState updateWithZero { x =>
      x.n += 1
      DataBag()
    }

    // constructor did deep copy, original should not change
    initial.fetch().head should be (Foo(1, 1))
    val result = withState.bag()
    // we see the change in the stateful
    result.fetch().head should be (Foo(1, 2))

    withState updateWithZero { x =>
      x.n += 1
      DataBag()
    }

    // `.bag()` did deep copy, the resulting `DataBag` is independent of the stateful
    result.fetch().head should be (Foo(1, 2))
  }
}

object StatefulNativeTest {
  case class Foo(@id s: Int, var n: Int) extends Identity[Int] {
    override def identity = n
  }
}
