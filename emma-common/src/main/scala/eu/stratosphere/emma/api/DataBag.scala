package eu.stratosphere.emma.api

import scala.language.experimental.macros

/**
 * An abstraction for homogeneous collections.
 */
sealed class DataBag[+A] private(private[api] val impl: Seq[A]) extends Serializable {

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  /**
   * Structural recursion over the bag. Assumes an algebraic specificaiton of the DataBag type using three constructors:
   *
   * {{
   * def Empty[A]: DataBag[A] // constructs an empty bag
   * def Sng[A](value: A): DataBag[A] // constructs a bag with one element
   * def Union[A](left: DataBag[A], right: DataBag[A]): DataBag[A] //
   * }}
   *
   * The specification of this function can be therefore interpreted as the following program:
   *
   * {{{
   * this match {
   *   case Empty => nil
   *   case Sng(value) => sng(value)
   *   case Union(left, right) => plus(left.fold(nil, sng, plus), right.fold(nil, sng, plus))
   * }
   * }}}
   *
   * @param z Substitute for Empty
   * @param s Substitute for Sng
   * @param p Substitute for Union
   * @tparam B The result type of the recursive computation
   * @return
   */
  def fold[B](z: B, s: A => B, p: (B, B) => B): B = impl.foldLeft(z)((acc, x) => p(s(x), acc))

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  /**
   * Monad map.
   *
   * @param f Function to be applied on the collection.
   * @tparam B Type of the output DataBag.
   * @return A DataBag containing the elements `f(x)` produced for each element x of the input.
   */
  def map[B](f: (A) => B): DataBag[B] = DataBag(impl.map(f))

  /**
   * Monad flatMap.
   *
   * @param f Function to be applied on the collection. The resulting bags are flattened.
   * @tparam B Type of the output DataBag.
   * @return A DataBag containing the union (flattening) of the DataBags `f(x)` produced for each element of the input.
   */
  def flatMap[B](f: (A) => DataBag[B]): DataBag[B] = DataBag(impl.flatMap(x => f(x).impl))

  /**
   * Monad filter.
   *
   * @param p Predicate to be applied on the collection. Only qualifying elements are passed down the chain.
   * @return
   */
  def withFilter(p: (A) => Boolean): DataBag[A] = DataBag(impl.filter(p))

  // -----------------------------------------------------
  // Grouping and Set operations
  // -----------------------------------------------------

  /**
   * Groups the bag by key.
   *
   * @param k Key selector function.
   * @tparam K Key type.
   * @return A version of this bag with the entries grouped by key.
   */
  def groupBy[K](k: (A) => K): DataBag[Group[K, DataBag[A]]] = for (key <- this.map(k).distinct()) yield Group(key, for (y <- this; if k(y) == key) yield y)

  /**
   * Difference operator. Respects duplicates, e.g.:
   *
   * {{{
   * DataBag(Seq(1,1,2,3)) minus DataBag(Seq(1,2,5)) = DataBag(Seq(1,3))
   * }}}
   *
   * @param subtrahend The subtrahend parameter.
   * @tparam B The type of the operator.
   * @return The set-theoretic difference (with duplicates) between this DataBag and the given subtrahend.
   */
  def minus[B >: A](subtrahend: DataBag[B]): DataBag[B] = DataBag(this.impl diff subtrahend.impl)

  /**
   * Plus operator (union). Respects duplicates, e.g.:
   *
   * {{{
   * DataBag(Seq(1,1,2,3)) plus DataBag(Seq(1,2,5)) = DataBag(Seq(1,1,2,3,1,2,5))
   * }}}
   *
   * @param addend The second addend parameter.
   * @tparam B The type of the operator.
   * @return The set-theoretic union (with duplicates) between this DataBag and the given subtrahend.
   */
  def plus[B >: A](addend: DataBag[B]): DataBag[B] = DataBag(this.impl ++ addend.impl)

  /**
   * Removes duplicate entries from the bag, e.g.
   *
   * {{{
   * DataBag(Seq(1,1,2,3)).distinct() = DataBag(Seq(1,2,3))
   * }}}
   *
   * @return A version of this DataBag without duplicate entries.
   */
  def distinct(): DataBag[A] = DataBag(impl.distinct)

  // -----------------------------------------------------
  // Conversion Methods
  // -----------------------------------------------------

  /**
   * Converts a DataBag abstraction back into a scala sequence.
   *
   * @return A filtered version of the DataBag.
   */
  def fetch(): Seq[A] = impl
}

object DataBag {

  /**
   * Empty constructor.
   *
   * @tparam A The element type for the DataBag.
   * @return An empty DataBag for elements of type A.
   */
  def apply[A](): DataBag[A] = new DataBag[A](List())

  /**
   * Sequence constructor.
   *
   * @param values The values contained in the bag.
   * @tparam A The element type for the DataBag.
   * @return A DataBag containing the elements of the `values` sequence.
   */
  def apply[A](values: Seq[A]): DataBag[A] = new DataBag[A](values)
}
