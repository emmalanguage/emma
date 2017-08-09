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
import model._

@emma.lib
object DirectorsMuses {

  def apply(input: String, csv: CSV): String => Unit = {

    val people: DataBag[Person] = DataBag.readCSV[Person](s"$input/people.csv", csv)
    val movies: DataBag[Movie] = DataBag.readCSV[Movie](s"$input/people.csv", csv)
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
      if cd.creditType == CreditType.Director.toString
      if ca.creditType == CreditType.Actor.toString
    } yield Collaboration(pd, pa, mv)

    val collaborationCounts = for {
      Group((d, a), ms) <- collaborations.groupBy { case Collaboration(d, a, _) => (d, a) }
    } yield CollaborationCount(d, a, ms.size)

    val maxCounts = collaborationCounts
      .groupBy(_.director)
      .map { case Group(d, vals) => MaxCount(d, vals.map(_.count).max) }

    val muses = for {
      cc <- collaborationCounts
      mc <- maxCounts
      if cc.director == mc.director
      if cc.count > 1
      if cc.count >= mc.maxCount - 1
    } yield cc

    // result function
    (name: String) => if (name.nonEmpty) {
      val matches = for {
        x <- muses; if x.director.name contains name
      } yield x

      for ((d, ccs) <- matches.collect().groupBy(_.director)) println(
        s"""
        |Director ${d.name} has the following muses:
        |${ccs.map(cc => s"- ${cc.actor.name} (collaborated in ${cc.count} movies").mkString("\n")}
        """.stripMargin)
    }
  }

  //----------------------------------------------------------------------------
  // helper model
  //----------------------------------------------------------------------------

  //@formatter:off
  case class Collaboration
  (
    director : Person,
    actor    : Person,
    movie    : Movie
  )

  case class CollaborationCount
  (
    director : Person,
    actor    : Person,
    count    : Long
  )

  case class MaxCount
  (
    director : Person,
    maxCount : Long
  )
  //@formatter:on

}
