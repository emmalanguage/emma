package eu.stratosphere.emma.codegen

object testschema {
  case class IMDBEntry(title: String, rating: Double, rank: Int, link: String, year: Int) {}
  case class FilmFestivalWinner(year: Int, title: String, director: String, country: String) {}
  case class EdgeWithLabel[VT, LT](src: VT, dst: VT, label: LT) {}
  case class Edge[VT](src: VT, dst: VT) {}
}
