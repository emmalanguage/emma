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
package org.emmalanguage
package repl

import api._

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class ObjectBasedScalaREPLSpec extends FlatSpec with Matchers {

  import ObjectBasedScalaREPLSpec._

  "object-based Scala REPL" should "compile and evaluate properly" in {
    $read$2.$iw.$iw.act should contain theSameElementsAs $read$1.$iw.$iw.exp
  }
}

object ObjectBasedScalaREPLSpec extends FlinkAware {

  object $read$1 extends scala.AnyRef {

    object $iw extends scala.AnyRef {

      object $iw extends scala.AnyRef {
        val exp = Seq(42)
      }

    }

  }

  object $read$2 extends scala.AnyRef {

    object $iw extends scala.AnyRef {

      import $read$1.$iw.$iw.exp

      object $iw extends scala.AnyRef {
        val act = withDefaultFlinkEnv(implicit flink => emma.onFlink {
          DataBag(exp).collect()
        })
      }

    }

  }

}
