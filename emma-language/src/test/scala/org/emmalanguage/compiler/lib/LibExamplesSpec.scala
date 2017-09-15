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

/** A collection of program examples to be used in the `Function*Spec` classes. */
class LibExamplesSpec extends LibExamples {

  // ---------------------------------------------------------------------------
  // Spec
  // ---------------------------------------------------------------------------

  "Alpha-equivalent definition for " - {

    "Example A (Emma Source)" in {
      idPipeline(`Example A (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example A (Emma Source)`))
    }

    "Example B (Emma Source)" in {
      idPipeline(`Example B (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example B (Emma Source)`))
    }

    "Example C (Emma Source)" in {
      idPipeline(`Example C (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example C (Emma Source)`))
    }

    "Example D (Emma Source)" in {
      idPipeline(`Example D (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example D (Emma Source)`))
    }

    "Example E (Emma Source)" in {
      idPipeline(`Example E (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example E (Emma Source)`))
    }

    "Example F (Emma Source)" in {
      idPipeline(`Example F (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example F (Emma Source)`))
    }

    "Example G (Emma Source)" in {
      idPipeline(`Example G (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example G (Emma Source)`))
    }

    "Example H (Emma Source)" in {
      idPipeline(`Example H (Original Expr)`) shouldBe alphaEqTo(idPipeline(`Example H (Emma Source)`))
    }
  }
}
