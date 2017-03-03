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

import converter.CollConverter
import io.csv._
import io.parquet._

import scala.language.higherKinds

/** An abstraction for homogeneous distributed collections. */
trait DataBag[A] extends Serializable {

  import Meta.Projections._

  implicit def m: Meta[A]

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
  def fetch(): Seq[A]

  /**
   * Converts this bag into a distributed collection of type `DColl[A]`.
   */
  def as[DColl[_]](implicit converter: CollConverter[DColl]): DColl[A] =
    converter(this)

  // -----------------------------------------------------
  // Pre-defined folds
  // -----------------------------------------------------

  /**
   * Test the collection for emptiness.
   *
   * @return `true` if the collection contains no elements at all
   */
  def isEmpty: Boolean =
    fold(true)(_ => false, _ && _)

  /**
   * Tet the collection for emptiness.
   *
   * @return `true` if the collection has at least one element
   */
  def nonEmpty: Boolean =
    fold(false)(_ => true, _ || _)

  /**
   * Shortcut for `fold(z)(identity, f)`.
   *
   * @param z `zero`: bottom (of the recursion) element
   * @param u `plus`: reducing (folding) function, should be associative
   * @tparam B return type (super class of the element type)
   * @return the result of combining all elements into one
   */
  def reduce[B >: A : Meta](z: B)(u: (B, B) => B): B =
    fold(z)(x => x, u)

  /**
   * Shortcut for `fold(None)(Some, Option.lift2(f))`, which is the same as reducing the collection
   * to a single element by applying a binary operator.
   *
   * @param p `plus`: reducing (folding) function, should be associative
   * @return the result of reducing all elements into one
   */
  def reduceOption(p: (A, A) => A): Option[A] =
    fold(Option.empty[A])(Some(_), (xo, yo) => (for {
      x <- xo
      y <- yo
    } yield p(x, y)) orElse xo orElse yo)

  /**
   * Find the smallest element in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param o the implicit natural [[Ordering]] of the elements
   * @throws Exception if the collection is empty
   */
  def min(implicit o: Ordering[A]): A =
    reduceOption(o.min).get

  /**
   * Find the largest element in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param o the implicit natural [[Ordering]] of the elements
   * @throws Exception if the collection is empty
   */
  def max(implicit o: Ordering[A]): A =
    reduceOption(o.max).get

  /**
   * Calculate the sum over all elements in the collection.
   *
   * @param n implicit [[Numeric]] operations of the elements
   * @return zero if the collection is empty
   */
  def sum(implicit n: Numeric[A]): A =
    reduce(n.zero)(n.plus)

  /**
   * Calculate the product over all elements in the collection.
   *
   * @param n implicit [[Numeric]] operations of the elements
   * @return one if the collection is empty
   */
  def product(implicit n: Numeric[A]): A =
    reduce(n.one)(n.times)

  /** @return the number of elements in the collection */
  def size: Long =
    fold(0L)(_ => 1L, _ + _)

  /**
   * Count the number of elements in the collection that satisfy a predicate.
   *
   * @param p the predicate to test against
   * @return the number of elements that satisfy `p`
   */
  def count(p: A => Boolean): Long =
    fold(0L)(p(_) compare false, _ + _)

  /**
   * Test if at least one element of the collection satisfies `p`.
   *
   * @param p predicate to test against the elements of the collection
   * @return `true` if the collection contains an element that satisfies the predicate
   */
  def exists(p: A => Boolean): Boolean =
    fold(false)(p, _ || _)

  /**
   * Test if all elements of the collection satisfy `p`.
   *
   * @param p predicate to test against the elements of the collection
   * @return `true` if all the elements of the collection satisfy the predicate
   */
  def forall(p: A => Boolean): Boolean =
    fold(true)(p, _ && _)

  /**
   * Finds some element in the collection that satisfies a given predicate.
   *
   * @param p the predicate to test against
   * @return [[Some]] element if one exists, [[None]] otherwise
   */
  def find(p: A => Boolean): Option[A] =
    fold(Option.empty[A])(Some(_) filter p, _ orElse _)

  /**
   * Find the bottom `n` elements in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param n number of elements to return
   * @param o the implicit [[Ordering]] of elements
   * @return an ordered (ascending) [[List]] of the bottom `n` elements
   */
  def bottom(n: Int)(implicit o: Ordering[A]): List[A] =
    if (n <= 0) List.empty[A]
    else fold(List.empty[A])(_ :: Nil, DataBag.merge(n, _, _))

  /**
   * Find the top `n` elements in the collection with respect to the natural ordering of the
   * elements' type.
   *
   * @param n number of elements to return
   * @param o the implicit [[Ordering]] of elements
   * @return an ordered (descending) [[List]] of the bottom `n` elements
   */
  def top(n: Int)(implicit o: Ordering[A]): List[A] =
    bottom(n)(o.reverse)

  /**
   * Take a random sample of specified size.
   *
   * @param n number of elements to return
   * @return a [[List]] of `n` random elements
   */
  def sample(n: Int): List[A] = {
    val now = System.currentTimeMillis
    if (n <= 0) List.empty[A]
    else fold((List.empty[A], now))(
      (x: A) => (x :: Nil, x.hashCode), {
        case ((x, sx), (y, sy)) =>
          if (x.size + y.size <= n) (x ::: y, sx ^ sy)
          else {
            val seed = now ^ ((sx << Integer.SIZE) | sy)
            val rand = new _root_.scala.util.Random(seed)
            (rand.shuffle(x ::: y).take(n), rand.nextLong())
          }
      })._1
  }

  // -----------------------------------------------------
  // equals and hashCode
  // -----------------------------------------------------

  override def equals(o: Any): Boolean = o match {
    case that: DataBag[A] =>
      lazy val hashEq = this.## == that.##
      lazy val thisVals = this.fetch()
      lazy val thatVals = that.fetch()
      // Note that in the following line, checking diff in only one direction is enough because we also compare the
      // sizes. Also note that SeqLike.diff uses bag semantics.
      lazy val valsEq = thisVals.size == thatVals.size && (thisVals diff thatVals).isEmpty
      hashEq && valsEq
    case _ =>
      false
  }

  override def hashCode(): Int =
    scala.util.hashing.MurmurHash3.unorderedHash(fetch())

}

trait DataBagCompanion[E] {

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

object DataBag extends DataBagCompanion[ScalaEnv] {

  // -----------------------------------------------------
  // Constructors & Sources
  // -----------------------------------------------------

  def empty[A: Meta](implicit env: ScalaEnv): DataBag[A] =
    ScalaSeq.empty[A]

  def apply[A: Meta](values: Seq[A])(implicit env: ScalaEnv): DataBag[A] =
    ScalaSeq(values)

  def readText(path: String)(implicit env: ScalaEnv): DataBag[String] =
    ScalaSeq.readText(path)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit env: ScalaEnv): DataBag[A] =
    ScalaSeq.readCSV[A](path, format)

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(implicit env: ScalaEnv): DataBag[A] =
    ScalaSeq.readParquet(path, format)

  // -----------------------------------------------------
  // Helper methods
  // -----------------------------------------------------

  import Ordering.Implicits._

  @scala.annotation.tailrec
  private def merge[A: Ordering](n: Int, l1: List[A], l2: List[A], acc: List[A] = Nil): List[A] =
    if (acc.length == n) acc.reverse
    else (l1, l2) match {
      case (x :: t1, y :: _) if x <= y => merge(n, t1, l2, x :: acc)
      case (_, y :: t2) => merge(n, l1, t2, y :: acc)
      case (_, Nil) => acc.reverse ::: l1.take(n - acc.length)
      case (Nil, _) => acc.reverse ::: l2.take(n - acc.length)
      case _ => acc.reverse
    }
}
