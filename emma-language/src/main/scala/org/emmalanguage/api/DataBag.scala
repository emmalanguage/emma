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
package org.emmalanguage
package api

import io.csv.{CSV, CSVConverter}

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
   * Writes a DataBag into the specified `path` in a CSV format.
   *
   * @param path      The location where the data will be written.
   * @param format    The CSV format configuration
   * @param converter A converter to use for element serialization.
   */
  def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit

  /**
   * Converts a DataBag abstraction back into a scala sequence.
   *
   * @return The contents of the DataBag as a scala sequence.
   */
  def fetch(): Traversable[A]
}

object DataBag {

  // -----------------------------------------------------
  // Constructors & Sources
  // -----------------------------------------------------

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
   * Reads a DataBag into the specified `path` using in a CSV format.
   *
   * @param path   The location where the data will be read from.
   * @param format The CSV format configuration.
   * @tparam A the type of elements to read.
   */
  def readCSV[A: Meta : CSVConverter](path: String, format: CSV): DataBag[A] = ScalaTraversable.readCSV[A](path, format)
}