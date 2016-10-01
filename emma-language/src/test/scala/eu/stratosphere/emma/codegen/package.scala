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
package eu.stratosphere.emma

import eu.stratosphere.emma.api.model.Identity

package object codegen {
  // schema
  case class ImdbMovie(title: String, rating: Double, rank: Int, link: String, year: Int)
  case class ImdbYear(year: Int)
  case class FilmFestWinner(year: Int, title: String, director: String, country: String)
  case class LabelledEdge[V, L](src: V, dst: V, label: L)
  case class Edge[V](src: V, dst: V)
  case class State(identity: Long, var value: Int) extends Identity[Long]
  case class Update(identity: Long, inc: Int) extends Identity[Long]

  // predicates
  def p1(x: Int) = x < 2
  def p2(x: Int) = x > 5
  def p3(x: Int) = x == 0
}
