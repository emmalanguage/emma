#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.graphs

import model._

import org.emmalanguage.api._

@emma.lib
object TransitiveClosure {

  def apply[V: Meta](edges: DataBag[Edge[V]]): DataBag[Edge[V]] = {
    var paths = edges.distinct
    var count = paths.size
    var added = 0L

    do {
      val delta = for {
        e1 <- paths
        e2 <- paths
        if e1.dst == e2.src
      } yield Edge(e1.src, e2.dst)

      paths = (paths union delta).distinct

      added = paths.size - count
      count = paths.size
    } while (added > 0)

    paths
  }
}
