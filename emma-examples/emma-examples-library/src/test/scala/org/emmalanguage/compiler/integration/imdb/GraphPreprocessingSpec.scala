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
package compiler.integration.imdb

import api._
import compiler.ir.ComprehensionSyntax._
import compiler.integration.BaseCompilerIntegrationSpec
import examples.graphs.model._
import examples.imdb.GraphPreprocessing
import examples.imdb.GraphPreprocessing.Collaboration
import examples.imdb.model._

class GraphPreprocessingSpec extends BaseCompilerIntegrationSpec {

  import compiler._
  import u.reify

  // ---------------------------------------------------------------------------
  // Program closure
  // ---------------------------------------------------------------------------

  // input parameters
  val input: String = null
  val output: String = null
  val csv = CSV()
  implicit val pCSVConverter = CSVConverter[Person]
  implicit val mCSVConverter = CSVConverter[Movie]
  implicit val cCSVConverter = CSVConverter[Credit]
  implicit val eCSVConverter = CSVConverter[LEdge[Person, Long]]

  val sourceExpr = liftPipeline(reify {
    val edges = GraphPreprocessing(input, csv)(_.count(c => c.director == c.actor))
    edges.writeCSV(output, csv)
  })

  val coreExpr = anfPipeline(reify {
    val input$r1 = this.input
    val csv$r1 = this.csv
    val apply$r1 = StringContext.apply("", "/people.csv")
    val s$r1 = apply$r1.s(input$r1)
    val people$r1 = DataBag.readCSV[Person](s$r1, csv$r1)
    val apply$r2 = StringContext.apply("", "/people.csv")
    val s$r2 = apply$r2.s(input$r1)
    val movies$r1 = DataBag.readCSV[Movie](s$r2, csv$r1)
    val apply$r3 = StringContext.apply("", "/people.csv")
    val s$r3 = apply$r3.s(input$r1)
    val credits$r1 = DataBag.readCSV[Credit](s$r3, csv$r1)
    val collaborations$r1 = comprehension[Collaboration, DataBag]({
      val pd$r2 = generator[Person, DataBag]({
        people$r1
      })
      val cd$r1 = generator[Credit, DataBag]({
        credits$r1
      })
      val mv$r1 = generator[Movie, DataBag]({
        movies$r1
      })
      val ca$r1 = generator[Credit, DataBag]({
        credits$r1
      })
      val pa$r2 = generator[Person, DataBag]({
        people$r1
      })
      guard({
        val id$r1 = pd$r2.id
        val personID$r1 = cd$r1.personID
        val `==$r1` = id$r1.==(personID$r1)
        `==$r1`
      })
      guard({
        val id$r2 = mv$r1.id
        val movieID$r1 = cd$r1.movieID
        val `==$r2` = id$r2.==(movieID$r1)
        `==$r2`
      })
      guard({
        val id$r3 = mv$r1.id
        val movieID$r2 = ca$r1.movieID
        val `==$r3` = id$r3.==(movieID$r2)
        `==$r3`
      })
      guard({
        val id$r4 = pa$r2.id
        val personID$r2 = ca$r1.personID
        val `==$r4` = id$r4.==(personID$r2)
        `==$r4`
      })
      guard({
        val creditType$r1: cd$r1.creditType.type = cd$r1.creditType
        val `==$r5` = creditType$r1.==("director")
        `==$r5`
      })
      guard({
        val creditType$r2: ca$r1.creditType.type = ca$r1.creditType
        val `==$r6` = creditType$r2.==("actor")
        `==$r6`
      })
      head[Collaboration]({
        val apply$r4 = Collaboration.apply(pd$r2, cd$r1, mv$r1, ca$r1, pa$r2)
        apply$r4
      })
    })
    val anonfun$r12 = (c$r1: Collaboration) => {
      val director$r1: c$r1.director.type = c$r1.director
      val actor$r1: c$r1.actor.type = c$r1.actor
      val apply$r5 = Tuple2.apply[Person, Person](director$r1, actor$r1)
      apply$r5
    }
    val groupBy$r1 = collaborations$r1.groupBy[(Person, Person)](anonfun$r12)
    val edges = comprehension[LEdge[Person, Long], DataBag]({
      val check$ifrefutable$1$r1 = generator[Group[(Person, Person), DataBag[Collaboration]], DataBag]({
        groupBy$r1
      })
      head[LEdge[Person, Long]]({
        val key$r1: check$ifrefutable$1$r1.key.type = check$ifrefutable$1$r1.key
        val pd$r1 = key$r1._1
        val key$r2: check$ifrefutable$1$r1.key.type = check$ifrefutable$1$r1.key
        val pa$r6 = key$r2._2
        val cs$r1 = check$ifrefutable$1$r1.values
        val anonfun$r14 = (c: Collaboration) => {
          val director$r2: c.director.type = c.director
          val actor$r2: c.actor.type = c.actor
          val `==$r7` = director$r2.==(actor$r2)
          `==$r7`
        }
        val apply$r6 = cs$r1.count(anonfun$r14)
        val apply$r7 = LEdge(pd$r1, pa$r6, apply$r6)
        apply$r7
      })
    })
    val output$r1: this.output.type = this.output
    val csv$r3: this.csv.type = this.csv
    val writeCSV$r1 = edges.writeCSV(output$r1, csv$r3)
    writeCSV$r1
  })

  // ---------------------------------------------------------------------------
  // Specs
  // ---------------------------------------------------------------------------

  "lifting" in {
    sourceExpr shouldBe alphaEqTo(coreExpr)
  }
}
