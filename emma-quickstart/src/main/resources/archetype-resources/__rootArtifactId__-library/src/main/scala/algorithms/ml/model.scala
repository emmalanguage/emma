#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.ml

import org.emmalanguage.api._

/** Machine learning model objects. */
object model {
  case class Point[ID](@emma.pk id: ID, pos: Array[Double])
  case class LPoint[ID, L](@emma.pk id: ID, pos: Array[Double], label: L)
}
