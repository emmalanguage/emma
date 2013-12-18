package eu.stratosphere.emma

/**
 * The base type of all intermediate results in the batch-processing part of the algebra. DataSets inherit from DataStream[T]
 * because bag (or set) semantics can be formally seen as e restriction of stream (list) semantics.
 *
 * Algebra operators that are well defined on e DataStream are consequently also well defined on DataSet, but the
 * reverse does not necessarily hold.
 */
class DataSet[T] {

  def map[B](f: (T) => B): DataSet[B] = ???

  def withFilter(p: (T) => Boolean): DataSet[T] = ???

  def flatMap[B](f: (T) => DataSet[B]): DataSet[B] = ???
}

object DataSet {
  def apply[T](): DataSet[T] = new DataSet[T]()
}
