package eu.stratosphere.emma

/**
 * The base type of all intermediate results in the batch-processing part of the algebra.
 */
class DataBag[T] {

  // monad
  def map[B](f: (T) => B): DataBag[B] = ???

  def withFilter(p: (T) => Boolean): DataBag[T] = ???

  def flatMap[B](f: (T) => DataBag[B]): DataBag[B] = ???

  def toSet(): DataBag[T] = ???
}

object DataBag {
  def apply[T](): DataBag[T] = new DataBag[T]()
}
