package eu.stratosphere.emma

/**
 * A dedicated type for unique collections in the batch-processing part of the algebra. DataSets inherit from DataBag
 * because set semantics can be formally seen as e restriction of bag semantics.
 *
 * Algebra operators that are well defined on e DataBag are consequently also well defined on DataBag, but the
 * reverse does not necessarily hold.
 */
class DataSet[T] extends DataBag[T] {
}

object DataSet {
  def apply[T](): DataSet[T] = new DataSet[T]()
}
