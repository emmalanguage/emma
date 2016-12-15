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
package util

import cats.Monoid
import quiver.Graph
import shapeless._

import scala.collection.SortedSet
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds

/** Missing instances of [[cats.Monoid]]. */
object Monoids {

  /** Conjunctive boolean monoid. */
  val conj: Monoid[Boolean] = new Monoid[Boolean] {
    def empty = true
    def combine(x: Boolean, y: Boolean) = x && y
  }

  /** Disjunctive boolean monoid. */
  val disj: Monoid[Boolean] = new Monoid[Boolean] {
    def empty: Boolean = false
    def combine(x: Boolean, y: Boolean) = x || y
  }

  /** [[shapeless.HNil]] is an empty monoid. */
  implicit val hnil: Monoid[HNil] = new Monoid[HNil] {
    def empty = HNil
    def combine(x: HNil, y: HNil) = HNil
  }

  /** [[shapeless.HList]] is a generic product. */
  implicit def hcons[H, T <: HList](implicit H: Monoid[H], T: Monoid[T])
    : Monoid[H :: T] = new Monoid[H :: T] {
      val empty = H.empty :: T.empty
      def combine(x: H :: T, y: H :: T) =
        H.combine(x.head, y.head) :: T.combine(x.tail, y.tail)
    }

  /** Trivial monoid with bias to the left. */
  def first[A](zero: A): Monoid[A] = new Monoid[A] {
    def empty = zero
    def combine(x: A, y: A) = if (x == zero) y else x
  }

  /** Trivial monoid with bias to the right. */
  def last[A](zero: A): Monoid[A] = new Monoid[A] {
    def empty = zero
    def combine(x: A, y: A) = if (y == zero) x else y
  }

  /** Monoid for maps that overwrites previous values. */
  def overwrite[K, V]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    def empty = Map.empty
    def combine(x: Map[K, V], y: Map[K, V]) = x ++ y
  }

  /** Monoid for maps that merges previous values via an underlying monoid. */
  def merge[K, V](implicit V: Monoid[V]): Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    val empty = Map.empty[K, V].withDefaultValue(V.empty)
    def combine(x: Map[K, V], y: Map[K, V]) =
      y.foldLeft(x.withDefaultValue(V.empty)) {
        case (map, (k, v)) => map + (k -> V.combine(map(k), v))
      }
  }

  /** Monoid for collections that preserves only the last `n` elements. */
  def sliding[Col[x] <: Iterable[x], A](n: Int)
    (implicit Col: CanBuildFrom[Nothing, A, Col[A]])
    : Monoid[Col[A]] = new Monoid[Col[A]] {
      val empty = Col().result()
      def combine(x: Col[A], y: Col[A]) = x ++ y takeRight n to Col
    }

  /** Monoid for sorted sets. */
  implicit def sortedSet[A: Ordering]: Monoid[SortedSet[A]] = new Monoid[SortedSet[A]] {
    val empty = SortedSet.empty
    def combine(x: SortedSet[A], y: SortedSet[A]) = x | y
  }

  /** Reverses an existing monoid (i.e. applying it right to left). */
  def reverse[A](implicit A: Monoid[A]): Monoid[A] = new Monoid[A] {
    def empty = A.empty
    def combine(x: A, y: A) = A.combine(y, x)
  }

  /** Graph union forms a monoid. */
  implicit def graphUnion[V, A, B]: Monoid[Graph[V, A, B]] = new Monoid[Graph[V, A, B]] {
    def empty = quiver.empty[V, A, B]
    def combine(x: Graph[V, A, B], y: Graph[V, A, B]) = x union y
  }
}
