package eu.stratosphere.emma.codegen.flink

object TestSchema {

  // --------------------------------------------------------------------------
  // Graphs
  // --------------------------------------------------------------------------

  case class Edge[VT](src: VT, dst: VT) {}

  case class EdgeWithLabel[VT, LT](src: VT, dst: VT, label: LT) {}

  // --------------------------------------------------------------------------
  // Cinema
  // --------------------------------------------------------------------------

  case class FilmFestivalWinner(year: Int, title: String, director: String, country: String) {}

}
