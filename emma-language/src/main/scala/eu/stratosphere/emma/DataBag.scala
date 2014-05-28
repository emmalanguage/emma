package eu.stratosphere.emma

import _root_.eu.stratosphere.emma.macros.algebra.AlgebraMacros

/**
 * The base type of all intermediate results in the batch-processing part of the algebra.
 */
class DataBag[T] {

  import scala.language.experimental.macros

  // monad
  def map[B](f: (T) => B): DataBag[B] = ???

  def withFilter(p: (T) => Boolean): DataBag[T] = ???

  def flatMap[B](f: (T) => DataBag[B]): DataBag[B] = ???

  def toSet(): DataSet[T] = ???

  def groupBy[K](k: T => K): DataBag[DataBag[T]] = macro AlgebraMacros.groupByMethod[T, K]
}

object DataBag {
  def apply[T](): DataBag[T] = new DataBag[T]()
}
