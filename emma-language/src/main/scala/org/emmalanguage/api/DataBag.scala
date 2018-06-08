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

import alg._

import scala.language.higherKinds

import java.util.UUID

/** An abstraction for homogeneous distributed collections. */
trait DataBag[A] extends Serializable {

  implicit def m: Meta[A]

  val uuid: UUID = UUID.randomUUID()

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
   * The function then denotes the following recursive computation:
   *
   * {{{
   * this match {
   *   case Empty => agg.zero
   *   case Sng(x) => agg.init(x)
   *   case Union(xs, ys) => p(xs.fold(agg), ys.fold(agg))
   * }
   * }}}
   *
   * @param agg The algebra parameterizing the recursion scheme.
   */
  def fold[B: Meta](agg: Alg[A, B]): B

  /** Delegates to `fold(Alg(zero, init, plus))`. */
  def fold[B: Meta](zero: B)(init: A => B, plus: (B, B) => B): B =
    fold(Fold(zero, init, plus))

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
  // Set Ops
  // -----------------------------------------------------

  /**
   * Union operator. Respects duplicates, e.g.:
   *
   * {{{
   * DataBag(Seq(1,1,2,3)) plus DataBag(Seq(1,2,5)) = DataBag(Seq(1,1,2,3,1,2,5))
   * }}}
   *
   * @param that The second addend parameter.
   * @return The set-theoretic union (with duplicates) between this DataBag and the given one.
   */
  def union(that: DataBag[A]): DataBag[A]

  /**
   * Removes duplicate entries from the bag, e.g.
   *
   * {{{
   * DataBag(Seq(1,1,2,3)).distinct = DataBag(Seq(1,2,3))
   * }}}
   *
   * @return A version of this DataBag without duplicate entries.
   */
  def distinct: DataBag[A]

  // -----------------------------------------------------
  // Partition-based Ops
  // -----------------------------------------------------

  /**
   * Creates a sample of up to `k` elements using reservoir sampling initialized with the given `seed`.
   *
   * If the collection represented by the [[DataBag]] instance contains less then `n` elements,
   * the resulting collection is trimmed to a smaller size.
   *
   * The method should be deterministic for a fixed [[DataBag]] instance with a materialized result.
   * In other words, calling `xs.sample(n)(seed)` two times in succession will return the same result.
   *
   * The result, however, might vary between program runs and [[DataBag]] implementations.
   */
  def sample(k: Int, seed: Long = 5394826801L): Vector[A]

  /**
   * Zips the elements of this collection with a unique dense index.
   *
   * The method should be deterministic for a fixed [[DataBag]] instance with a materialized result.
   * In other words, calling `xs.zipWithIndex()` two times in succession will return the same result.
   *
   * The result, however, might vary between program runs and [[DataBag]] implementations.
   */
  def zipWithIndex(): DataBag[(A, Long)]

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
   * Writes a DataBag into the specified `path` as plain text.
   *
   * The serialization logic is backend-specific.
   *
   * @param path The location where the data will be written.
   */
  def writeText(path: String): Unit

  /**
   * Writes a DataBag into the specified `path` in a CSV format.
   *
   * @param path      The location where the data will be written.
   * @param format    The CSV format configuration
   * @param converter A converter to use for element serialization.
   */
  def writeParquet(path: String, format: Parquet)(implicit converter: ParquetConverter[A]): Unit

  /**
   * Converts the DataBag back into a scala Seq.
   * Warning: Do not call this on DataBags that are too large to fit on one machine!
   *
   * @return The contents of the DataBag as a scala Seq.
   */
  def collect(): Seq[A]

  /**
   * Converts this bag into a distributed collection of type `DColl[A]`.
   */
  def as[DColl[_]](implicit conv: DataBag[A] => DColl[A]): DColl[A] =
    conv(this)

  // -----------------------------------------------------
  // Pre-defined folds
  // -----------------------------------------------------

  /**
   * Test the collection for emptiness.
   *
   * @return `true` if the collection contains no elements at all
   */
  def isEmpty: Boolean =
    fold(IsEmpty)

  /**
   * Tet the collection for emptiness.
   *
   * @return `true` if the collection has at least one element
   */
  def nonEmpty: Boolean =
    fold(NonEmpty)

  /**
   * Shortcut for `fold(z)(identity, f)`.
   *
   * @param zero: bottom (of the recursion) element
   * @param plus: reducing (merging) function, should be associative
   * @tparam B return type (super class of the element type)
   * @return the result of combining all elements into one
   */
  def reduce[B >: A : Meta](zero: B)(plus: (B, B) => B): B =
    fold(Reduce(zero, plus))

  /**
   * Shortcut for `fold(None)(Some, Option.lift2(f))`, which is the same as reducing the collection
   * to a single element by applying a binary operator.
   *
   * @param plus: reducing (merging) function, should be associative
   * @return the result of reducing all elements into one
   */
  def reduceOption(plus: (A, A) => A): Option[A] =
    fold(ReduceOpt(plus))

  /**
   * Find the smallest element in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param o the implicit natural [[Ordering]] of the elements
   * @throws Exception if the collection is empty
   */
  def min(implicit o: Ordering[A]): A =
    fold(Min(o)).get

  /**
   * Find the largest element in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param o the implicit natural [[Ordering]] of the elements
   * @throws Exception if the collection is empty
   */
  def max(implicit o: Ordering[A]): A =
    fold(Max(o)).get

  /**
   * Calculate the sum over all elements in the collection.
   *
   * @param n implicit [[Numeric]] operations of the elements
   * @return zero if the collection is empty
   */
  def sum(implicit n: Numeric[A]): A =
    fold(Sum(n))

  /**
   * Calculate the product over all elements in the collection.
   *
   * @param n implicit [[Numeric]] operations of the elements
   * @return one if the collection is empty
   */
  def product(implicit n: Numeric[A]): A =
    fold(Product(n))

  /** @return the number of elements in the collection */
  def size: Long =
    fold(Size)

  /**
   * Count the number of elements in the collection that satisfy a predicate.
   *
   * @param p the predicate to test against
   * @return the number of elements that satisfy `p`
   */
  def count(p: A => Boolean): Long =
    fold(Count(p))

  /**
   * Test if at least one element of the collection satisfies `p`.
   *
   * @param p predicate to test against the elements of the collection
   * @return `true` if the collection contains an element that satisfies the predicate
   */
  def exists(p: A => Boolean): Boolean =
    fold(Exists(p))

  /**
   * Test if all elements of the collection satisfy `p`.
   *
   * @param p predicate to test against the elements of the collection
   * @return `true` if all the elements of the collection satisfy the predicate
   */
  def forall(p: A => Boolean): Boolean =
    fold(Forall(p))

  /**
   * Finds some element in the collection that satisfies a given predicate.
   *
   * @param p the predicate to test against
   * @return [[Some]] element if one exists, [[None]] otherwise
   */
  def find(p: A => Boolean): Option[A] =
    fold(Find(p))

  /**
   * Find the bottom `n` elements in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param n number of elements to return
   * @param o the implicit [[Ordering]] of elements
   * @return an ordered (ascending) [[List]] of the bottom `n` elements
   */
  def bottom(n: Int)(implicit o: Ordering[A]): List[A] =
    fold(Bottom(n, o))

  /**
   * Find the top `n` elements in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param n number of elements to return
   * @param o the implicit [[Ordering]] of elements
   * @return an ordered (descending) [[List]] of the top `n` elements
   */
  def top(n: Int)(implicit o: Ordering[A]): List[A] =
    fold(Top(n, o))

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any): Boolean = o match {
    case that: DataBag[A] =>
      lazy val hashEq = this.## == that.##
      lazy val thisVals = this.collect()
      lazy val thatVals = that.collect()
      // Note that in the following line, checking diff in only one direction is enough because we also compare the
      // sizes. Also note that SeqLike.diff uses bag semantics.
      lazy val valsEq = thisVals.size == thatVals.size && (thisVals diff thatVals).isEmpty
      hashEq && valsEq
    case _ =>
      false
  }

  override def hashCode(): Int =
    scala.util.hashing.MurmurHash3.unorderedHash(collect())

  override def toString: String =
    getClass.getName + "@" + uuid;
}

trait DataBagCompanion[E] {

  /**
   * Distributed collection constructor.
   *
   * @param coll A distributed collection with input values.
   * @param conv A distributed collection converter.
   * @tparam A The element type for the DataBag.
   */
  def from[DColl[_], A](coll: DColl[A])(implicit conv: DColl[A] => DataBag[A]): DataBag[A] =
    conv(coll)

  /**
   * Empty constructor.
   *
   * @param env An execution environment instance.
   * @tparam A The element type for the DataBag.
   */
  def empty[A: Meta](implicit env: E): DataBag[A]

  /**
   * Sequence constructor.
   *
   * @param values The values contained in the bag.
   * @param env An execution environment instance.
   * @tparam A The element type for the DataBag.
   */
  def apply[A: Meta](values: Seq[A])(implicit env: E): DataBag[A]

  /**
   * Reads a `DataBag[String]` elements from the specified `path`.
   *
   * @param env  An execution environment instance.
   * @param path The location where the data will be read from.
   */
  def readText(path: String)(implicit env: E): DataBag[String]

  /**
   * Reads a DataBag from the specified `path` using in a CSV format.
   *
   * @param path   The location where the data will be read from.
   * @param format The CSV format configuration.
   * @param env    An execution environment instance.
   * @tparam A the type of elements to read.
   */
  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit env: E): DataBag[A]

  /**
   * Reads a DataBag into the specified `path` using a Parquet format.
   *
   * @param path   The location where the data will be read from.
   * @param format The Parquet format configuration.
   * @param env    An execution environment instance.
   * @tparam A the type of elements to read.
   */
  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(implicit env: E): DataBag[A]
}

object DataBag extends DataBagCompanion[LocalEnv] {

  // -----------------------------------------------------
  // Constructors & Sources
  // -----------------------------------------------------

  def empty[A: Meta](implicit env: LocalEnv): DataBag[A] =
    ScalaSeq.empty[A]

  def apply[A: Meta](values: Seq[A])(implicit env: LocalEnv): DataBag[A] =
    ScalaSeq(values)

  def readText(path: String)(implicit env: LocalEnv): DataBag[String] =
    ScalaSeq.readText(path)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit env: LocalEnv): DataBag[A] =
    ScalaSeq.readCSV[A](path, format)

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(implicit env: LocalEnv): DataBag[A] =
    ScalaSeq.readParquet(path, format)
}
