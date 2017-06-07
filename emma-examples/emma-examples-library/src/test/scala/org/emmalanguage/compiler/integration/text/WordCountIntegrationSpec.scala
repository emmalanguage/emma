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
package compiler.integration.text

import api._
import compiler.integration.BaseCompilerIntegrationSpec
import compiler.ir.ComprehensionSyntax._
import examples.text.WordCount

class WordCountIntegrationSpec extends BaseCompilerIntegrationSpec {

  import compiler._
  import u.reify

  // ---------------------------------------------------------------------------
  // Program closure
  // ---------------------------------------------------------------------------

  // input parameters
  val input: String = null
  val output: String = null
  val csv = CSV()
  implicit val countCSVConverter = CSVConverter[(String, Long)]

  // ---------------------------------------------------------------------------
  // Program representations
  // ---------------------------------------------------------------------------

  val sourceExpr = liftPipeline(reify {
    // read the input files and split them into lowercased words
    val docs = DataBag.readCSV[String](input, csv)
    // parse and count the words
    val counts = WordCount(docs)
    // write the results into a CSV file
    counts.writeCSV(output, csv)
  })

  val coreExpr = anfPipeline(reify {
    val input:  this.input.type = this.input
    val csv$r1: this.csv.type   = this.csv
    val docs = DataBag.readCSV[String](input, csv$r1)

    // read the input files and split them into lowercased words
    val words = comprehension[String, DataBag] {
      val line = generator[String, DataBag] {
        docs
      }
      val word = generator[String, DataBag] {
        DataBag[String](line.toLowerCase.split("\\W+"))
      }
      guard {
        val nonEmpty = word != ""
        nonEmpty
      }
      head {
        word
      }
    }

    val anonfun$r3 = (x: String) => {
      val x$r3 = Predef identity x
      x$r3
    }

    val groupBy$r1 = words groupBy anonfun$r3

    // group the words by their identity and count the occurrence of each word
    val counts = comprehension[(String, Long), DataBag] {
      val group = generator[Group[String, DataBag[String]], DataBag] {
        groupBy$r1
      }
      head {
        (group.key, group.values.size)
      }
    }

    // write the results into a CSV file
    val output$r1: this.output.type = this.output
    val csv$r2:    this.csv.type    = this.csv
    val x$r1 = counts.writeCSV(output$r1, csv$r2)
    x$r1
  })

  // ---------------------------------------------------------------------------
  // Specs
  // ---------------------------------------------------------------------------

  "lifting" in {
    sourceExpr shouldBe alphaEqTo(coreExpr)
  }
}
