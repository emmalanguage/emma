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
package compiler.lang.source

import compiler.BaseCompilerSpec

/** A spec for the `Core.foreach2loop` transformation. */
class Foreach2LoopSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val foreach2loopPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true, withPre = false)(
      fixSymbolTypes,
      unQualifyStatics,
      normalizeStatements,
      Foreach2Loop.transform.timed
    ).compose(_.tree)

  "foreach" - {
    "without closure modification" in {
      val act = foreach2loopPipeline(reify {
        for (i <- 1 to 5) println(i)
      })

      val exp = idPipeline(reify {
        for (i <- 1 to 5) println(i)
      })

      act shouldBe alphaEqTo (exp)
    }

    "without argument access" in {
      val act = foreach2loopPipeline(reify {
        var x = 42
        for (_ <- 1 to 5) x += 1
      })

      val exp = idPipeline(reify {
        var x = 42; {
          val iter$1 = 1.to(5).toIterator
          var _$1 = null.asInstanceOf[Int]
          while (iter$1.hasNext) {
            _$1 = iter$1.next
            x += 1
          }
        }
      })

      act shouldBe alphaEqTo (exp)
    }

    "with argument access" in {
      val act = foreach2loopPipeline(reify {
        var x = 42
        for (i <- 1 to 5) x += i
      })

      val exp = idPipeline(reify {
        var x = 42; {
          val iter$1 = 1.to(5).toIterator
          var i$1 = null.asInstanceOf[Int]
          while (iter$1.hasNext) {
            i$1 = iter$1.next
            x += i$1
          }
        }
      })

      act shouldBe alphaEqTo (exp)
    }

    "with monadic filter" in {
      val act = foreach2loopPipeline(reify {
        var x = 42
        for (i <- 1 to 10 if i % 2 == 0) x += i
      })

      val exp = idPipeline(reify {
        var x = 42; {
          val iter$1 = 1.to(10)
            .withFilter(_ % 2 == 0)
            .map(x => x).toIterator

          var i$1 = null.asInstanceOf[Int]
          while (iter$1.hasNext) {
            i$1 = iter$1.next
            x += i$1
          }
        }
      })

      act shouldBe alphaEqTo (exp)
    }
  }
}
