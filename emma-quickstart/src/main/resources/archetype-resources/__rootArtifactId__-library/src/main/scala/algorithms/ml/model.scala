#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.ml

import breeze.linalg.{Vector => Vec}
import org.emmalanguage.api._

/** Machine learning model objects. */
object model {
  case class Point[ID](@emma.pk id: ID, vector: Vec[Double])
  case class LVector[L](label: L, vector: Vec[Double])
}
