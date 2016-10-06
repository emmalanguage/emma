/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.stratosphere.emma.macros

import scala.language.experimental.macros

/**
 * Predefined aggregate functions on top of the `fold(zero)(sng, plus)` primitive, which
 * collections that implement the trait need to provide.
 * @tparam E the type of elements to fold over
 */
trait Folds[+E] extends Any {

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
   * Shortcut for `fold(z)(identity, f)`.
   * @param z `zero`: bottom (of the recursion) element
   * @param p `plus`: reducing (folding) function, should be associative
   * @tparam R return type (super class of the element type)
   * @return the result of combining all elements into one
   */
  def reduce[R >: E](z: R)(p: (R, R) => R): R =
    macro FoldMacros.reduce[R]

  /**
   * Shortcut for `fold(None)(Some, Option.lift2(f))`, which is the same as reducing the collection
   * to a single element by applying a binary operator.
   * @param p `plus`: reducing (folding) function, should be associative
   * @return the result of reducing all elements into one
   */
  def reduceOption(p: (E, E) => E): Option[E] =
    macro FoldMacros.reduceOption[E]

  /**
   * Find the smallest element in the collection with respect to the natural ordering of the
   * elements' type.
   * @param o the implicit natural [[Ordering]] of the elements
   * @throws Exception if the collection is empty
   */
  def min(implicit o: Ordering[E]): E =
    macro FoldMacros.min[E]

  /**
   * Find the largest element in the collection with respect to the natural ordering of the
   * elements' type.
   * @param o the implicit natural [[Ordering]] of the elements
   * @throws Exception if the collection is empty
   */
  def max(implicit o: Ordering[E]): E =
    macro FoldMacros.max[E]

  /**
   * Calculate the sum over all elements in the collection.
   * @param n implicit [[Numeric]] operations of the elements
   * @return zero if the collection is empty
   */
  def sum(implicit n: Numeric[E]): E =
    macro FoldMacros.sum[E]

  /**
   * Calculate the product over all elements in the collection.
   * @param n implicit [[Numeric]] operations of the elements
   * @return one if the collection is empty
   */
  def product(implicit n: Numeric[E]): E =
    macro FoldMacros.product[E]

  /** @return the number of elements in the collection */
  def size: Long = macro FoldMacros.size

  /**
   * Count the number of elements in the collection that satisfy a predicate.
   * @param p the predicate to test against
   * @return the number of elements that satisfy `p`
   */
  def count(p: E => Boolean): Long =
    macro FoldMacros.count[E]

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

  /**
    * Take a random sample of specified size.
    * @param n number of elements to return
    * @return a [[List]] of `n` random elements
    */
  def sample(n: Int): List[E] =
    macro FoldMacros.sample[E]

  /**
    * Write each element to a file in CSV format.
    * @param location the path or URL of the destination file
    */
  def writeCsv(location: String): Unit =
    macro FoldMacros.writeCsv[E]
}
