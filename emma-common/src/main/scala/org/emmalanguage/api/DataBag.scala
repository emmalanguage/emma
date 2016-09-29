package org.emmalanguage
package api

/** An abstraction for homogeneous distributed collections. */
trait DataBag[A] extends Serializable {

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  /**
   * Structural recursion over the bag.
   * Assumes an algebraic specification of the DataBag type using three constructors:
   *
   * {{{
   * sealed trait DataBag[A]
   * case class Sng[A](x: A) extends DataBag[A]
   * case class Union[A](xs: DataBag[A], ys: Bag[A]) extends DataBag[A]
   * case object Empty extends DataBag[Nothing]
   * }}}
   *
   * The specification of this function can be therefore interpreted as the following program:
   *
   * {{{
   * this match {
   *   case Empty => z
   *   case Sng(x) => s(x)
   *   case Union(xs, ys) => p(xs.fold(z)(s, u), ys.fold(z)(s, u))
   * }
   * }}}
   *
   * @param z Substitute for Empty
   * @param s Substitute for Sng
   * @param u Substitute for Union
   * @tparam B The result type of the recursive computation
   * @return
   */
  def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B

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
  def map[B: Meta](f: (A) => B): DataBag[B]

  /**
   * Monad flatMap.
   *
   * @param f Function to be applied on the collection. The resulting bags are flattened.
   * @tparam B Type of the output DataBag.
   * @return A DataBag containing the union (flattening) of the DataBags `f(x)` produced for each element of the input.
   */
  def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B]

  /**
   * Monad filter.
   *
   * @param p Predicate to be applied on the collection. Only qualifying elements are passed down the chain.
   * @return
   */
  def withFilter(p: (A) => Boolean): DataBag[A]

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  /**
   * Groups the bag by key.
   *
   * @param k Key selector function.
   * @tparam K Key type.
   * @return A version of this bag with the entries grouped by key.
   */
  def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]]

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  /**
   * Union operator. Respects duplicates, e.g.:
   *
   * {{{
   * DataBag(Seq(1,1,2,3)) plus DataBag(Seq(1,2,5)) = DataBag(Seq(1,1,2,3,1,2,5))
   * }}}
   *
   * @param that The second addend parameter.
   * @return The set-theoretic union (with duplicates) between this DataBag and the given subtrahend.
   */
  def union(that: DataBag[A]): DataBag[A]

  /**
   * Removes duplicate entries from the bag, e.g.
   *
   * {{{
   * DataBag(Seq(1,1,2,3)).distinct() = DataBag(Seq(1,2,3))
   * }}}
   *
   * @return A version of this DataBag without duplicate entries.
   */
  def distinct: DataBag[A]

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  /**
   * Converts a DataBag abstraction back into a scala sequence.
   *
   * @return The contents of the DataBag as a scala sequence.
   */
  def fetch(): Traversable[A]
}

object DataBag {

  /**
   * Empty constructor.
   *
   * @tparam A The element type for the DataBag.
   * @return An empty DataBag for elements of type A.
   */
  def apply[A: Meta]: DataBag[A] = ScalaTraversable[A]

  /**
   * Sequence constructor.
   *
   * @param values The values contained in the bag.
   * @tparam A The element type for the DataBag.
   * @return A DataBag containing the elements of the `values` sequence.
   */
  def apply[A: Meta](values: Seq[A]): DataBag[A] = ScalaTraversable(values)

  /**
   * 
   * @param name
   * @param f
   * @tparam A
   * @return
   */
  def time[A](name: String)(f: => A) = {
    val s = System.nanoTime
    val ret = f
    println(s"$name time: ${(System.nanoTime - s) / 1e6}ms".trim)
    ret
  }
}