package eu.stratosphere.emma.examples

import eu.stratosphere.emma.api.model._


package object graphs {

  // --------------------------------------------------------------------------
  // Schema
  // --------------------------------------------------------------------------

  case class VertexWithLabel[VT, LT](@id id: VT, label: LT) extends Identity[VT] {
    def identity = id
  }

  case class Edge[VT](@id src: VT, @id dst: VT) extends Identity[Edge[VT]] {
    def identity = Edge(src, dst)
  }

  case class EdgeWithLabel[VT, LT](@id src: VT, @id dst: VT, label: LT) extends Identity[Edge[VT]] {
    def identity = Edge(src, dst)
  }

}
