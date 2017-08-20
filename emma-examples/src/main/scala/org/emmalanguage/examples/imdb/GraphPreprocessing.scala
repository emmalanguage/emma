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
package examples.imdb

import api._
import examples.graphs.model._
import model._

@emma.lib
object GraphPreprocessing {

  type Proj[L] = DataBag[Collaboration] => L

  def apply[L: Meta](input: String, csv: CSV)(proj: Proj[L]): DataBag[LEdge[Person, L]] = {

    val people : DataBag[Person] = DataBag.readCSV[Person](s"$input/people.csv", csv)
    val movies : DataBag[Movie]  = DataBag.readCSV[Movie](s"$input/people.csv", csv)
    val credits: DataBag[Credit] = DataBag.readCSV[Credit](s"$input/people.csv", csv)

    val collaborations = for {
      pd <- people
      cd <- credits
      mv <- movies
      ca <- credits
      pa <- people
      if pd.id == cd.personID
      if mv.id == cd.movieID
      if mv.id == ca.movieID
      if pa.id == ca.personID
      if cd.creditType == "director" // CreditType.Director.toString
      if ca.creditType == "actor" // CreditType.Actor.toString
    } yield Collaboration(pd, cd, mv, ca, pa)

    for {
      Group((pd, pa), cs) <- collaborations.groupBy(c => (c.director, c.actor))
    } yield {
      LEdge(pd, pa, proj(cs))
    }
  }

  //----------------------------------------------------------------------------
  // helper model
  //----------------------------------------------------------------------------

  //@formatter:off
  case class Collaboration
  (
    director       : Person,
    creditDirector : Credit,
    movie          : Movie,
    creditActor    : Credit,
    actor          : Person
  )
  //@formatter:on

}
