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
package compiler.lib

import scala.tools.reflect.ToolBoxError

/** A spec for the `Lib.expand` function. */
class ExpandSpec extends LibExamples {

  import compiler._

  val prePipeline: u.Expr[Any] => u.Tree = compiler
    .pipeline(typeCheck = true, withPost = false)(
      TreeTransform("Owner.atEncl", api.Owner.atEncl)
    ).compose(_.tree)

  val expand: u.Tree => u.Tree =
    Lib.expand.timed

  "Exampe A" in {
    val inp = prePipeline(`Example A (Emma Source)`)
    val exp = prePipeline(`Example A (normalized)`)
    expand(inp) shouldBe alphaEqTo(exp)
  }

  "Exampe B" in {
    val inp = prePipeline(`Example B (Emma Source)`)
    assertThrows[ToolBoxError](expand(inp))
  }

  "Exampe C" in {
    val inp = prePipeline(`Example C (Emma Source)`)
    assertThrows[ToolBoxError](expand(inp))
  }

  "Exampe D" in {
    val inp = prePipeline(`Example D (Emma Source)`)
    assertThrows[ToolBoxError](expand(inp))
  }

  "Exampe E" in {
    val inp = prePipeline(`Example E (Emma Source)`)
    val exp = prePipeline(`Example E (normalized)`)
    expand(inp) shouldBe alphaEqTo(exp)
  }

  "Exampe F" in {
    val inp = prePipeline(`Example F (Emma Source)`)
    val exp = prePipeline(`Example F (normalized)`)
    expand(inp) shouldBe alphaEqTo(exp)
  }

  "Exampe G" in {
    val inp = prePipeline(`Example G (Emma Source)`)
    val exp = prePipeline(`Example G (normalized)`)
    expand(inp) shouldBe alphaEqTo(exp)
  }

  "Exampe H" in {
    val inp = prePipeline(`Example H (Emma Source)`)
    val exp = prePipeline(`Example H (normalized)`)
    expand(inp) shouldBe alphaEqTo(exp)
  }
}
