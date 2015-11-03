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
