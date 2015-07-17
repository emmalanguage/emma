package eu.stratosphere.emma.macros

import scala.language.experimental.macros

/**
 * Predefined aggregate functions on top of the `fold(zero, sng, plus)` primitive, which
 * collections that implement the trait need to provide.
 * @tparam E the type of elements to fold over
 */
trait Folds[+E] extends Any {

  def fold[R](z: R, s: E => R, p: (R, R) => R): R

  /**
   * Test the collection for emptiness.
   * @return `true` if the collection contains no elements at all
   */
  def isEmpty: Boolean =
    macro FoldMacros.isEmpty

  /**
   * Tet the collection for emptiness.
   * @return `true` if the collection has at least one element
   */
  def nonEmpty: Boolean =
    macro FoldMacros.nonEmpty

  /**
   * Alias for `fold(z, s, p)`. The same as applying a map (transformation) first, and then
   * reducing (folding) the result.
   * @see [[Folds#fold]]
   * @param z `zero`: bottom (of the recursion) element
   * @param s `sng`:  mapping (transforming) function
   * @param p `plus`: reducing (folding) function, should be associative
   * @tparam R result type of the map phase, fed into reduce
   * @return the result of reducing all elements into one
   */
  def fold3[R](z: R)(s: E => R, p: (R, R) => R): R =
    macro FoldMacros.fold3[E, R]

  /**
   * Shortcut for `fold(z, identity, f)`.
   * @see [Folds#fold]
   * @param z `zero`: bottom (of the recursion) element
   * @param p `plus`: reducing (folding) function, should be associative
   * @tparam R return type (super class of the element type)
   * @return the result of combining all elements into one
   */
  def fold2[R >: E](z: R)(p: (R, R) => R): R =
    macro FoldMacros.fold2[R]

  /**
   * Shortcut for `fold(None, Some, f)`, which is the same as reducing the collection to a single
   * element by applying a binary operator.
   * @see [Folds#fold]
   * @param p `plus`: reducing (folding) function, should be associative
   * @return the result of reducing all elements into one
   */
  def fold1(p: (E, E) => E): Option[E] =
    macro FoldMacros.fold1[E]

  /**
   * Same as `min` with a custom comparator function.
   * @see [Folds#min]
   * @param p predicate to be applied instead of `x < y`
   * @return the smallest element in the collection with respect to `p`
   */
  def minBy(p: (E, E) => Boolean): Option[E] =
    macro FoldMacros.minBy[E]

  /**
   * Same as `max` with a custom comparator function.
   * @see [Folds#max]
   * @param p predicate to be applied instead of `x < y`
   * @return the largest element in the collection with respect to `p`
   */
  def maxBy(p: (E, E) => Boolean): Option[E] =
    macro FoldMacros.maxBy[E]

  /**
   * Find the smallest element in the collection with respect to the natural ordering of the
   * elements' type.
   * @param o the implicit natural [[Ordering]] of the elements
   * @tparam R return type (super class of the element type)
   * @throws Exception if the collection is empty
   */
  def min[R >: E]()(implicit o: Ordering[R]): R =
    macro FoldMacros.min[R]

  /**
   * Find the largest element in the collection with respect to the natural ordering of the
   * elements' type.
   * @param o the implicit natural [[Ordering]] of the elements
   * @tparam R return type (super class of the element type)
   * @throws Exception if the collection is empty
   */
  def max[R >: E]()(implicit o: Ordering[R]): R =
    macro FoldMacros.max[R]

  /**
   * Find the smallest element in the collection after applying a function.
   * @see [[Folds#min]]
   * @param f the function to apply before comparing elements
   * @param o implicit [[Ordering]] of the result type of `f`
   * @tparam R the result type of `f`
   * @return the minimal element after applying `f`
   */
  def minWith[R](f: E => R)(implicit o: Ordering[R]): Option[E] =
    macro FoldMacros.minWith[E, R]

  /**
   * Find the largest element in the collection after applying a function.
   * @see [[Folds#max]]
   * @param f the function to apply before comparing elements
   * @param o implicit [[Ordering]] of the result type of `f`
   * @tparam R the result type of `f`
   * @return the maximal element after applying `f`
   */
  def maxWith[R](f: E => R)(implicit o: Ordering[R]): Option[E] =
    macro FoldMacros.maxWith[E, R]

  /**
   * Calculate the sum over all elements in the collection.
   * @param n implicit [[Numeric]] operations of the elements
   * @tparam R return type (super class of the element type)
   * @return zero if the collection is empty
   */
  def sum[R >: E]()(implicit n: Numeric[R]): R =
    macro FoldMacros.sum[R]

  /**
   * Calculate the product over all elements in the collection.
   * @param n implicit [[Numeric]] operations of the elements
   * @tparam R return type (super class of the element type)
   * @return one if the collection is empty
   */
  def product[R >: E]()(implicit n: Numeric[R]): R =
    macro FoldMacros.product[R]

  /**
   * Calculate the sum over all elements in the collection after applying a function.
   * @param f the function to apply before adding each element
   * @param n implicit [[Numeric]] operations of the elements
   * @tparam R return type of the sum function
   * @return zero if the collection is empty
   */
  def sumWith[R](f: E => R)(implicit n: Numeric[R]): R =
    macro FoldMacros.sumWith[E, R]

  /**
   * Calculate the product over all elements in the collection after applying a function.
   * @param f the function to apply before multiplying each element
   * @param n implicit [[Numeric]] operations of the elements
   * @tparam R return type of the product function
   * @return one if the collection is empty
   */
  def productWith[R](f: E => R)(implicit n: Numeric[R]): R =
    macro FoldMacros.productWith[E, R]

  /** @return the number of elements in the collection */
  def count(): Long = macro FoldMacros.count

  /** @return the number of elements in the collection */
  def size(): Long = macro FoldMacros.count

  /** @return the number of elements in the collection */
  def length(): Long = macro FoldMacros.count

  /**
   * Count the number of elements in the collection that satisfy a predicate.
   * @param p the predicate to test against
   * @return the number of elements that satisfy `p`
   */
  def countWith(p: E => Boolean): Long =
    macro FoldMacros.countWith[E]

  /**
   * Test if at least one element of the collection satisfies `p`.
   * @param p predicate to test against the elements of the collection
   * @return `false` if the collections is empty
   */
  def exists(p: E => Boolean): Boolean =
    macro FoldMacros.exists[E]

  /**
   * Test if all elements of the collection satisfy `p`.
   * @param p predicate to test against the elements of the collection
   * @return `true` if the collection is empty
   */
  def forall(p: E => Boolean): Boolean =
    macro FoldMacros.forall[E]

  /**
   * Find the bottom `n` elements in the collection with respect to the natural ordering of the
   * elements' type.
   * @param n number of elements to return
   * @param o the implicit [[Ordering]] of elements
   * @return an ordered (ascending) [[List]] of the bottom `n` elements
   */
  def bottom(n: Int)(implicit o: Ordering[E]): List[E] =
    macro FoldMacros.bottom[E]

  /**
   * Find the top `n` elements in the collection with respect to the natural ordering of the
   * elements' type.
   * @param n number of elements to return
   * @param o the implicit [[Ordering]] of elements
   * @return an ordered (descending) [[List]] of the bottom `n` elements
   */
  def top(n: Int)(implicit o: Ordering[E]): List[E] =
    macro FoldMacros.top[E]

  /**
   * Find the some element in the collection that satisfies a given predicate.
   * @param p the predicate to test against
   * @return [[Some]] element if one exists, [[None]] otherwise
   */
  def find(p: E => Boolean): Option[E] =
    macro FoldMacros.find[E]
}
