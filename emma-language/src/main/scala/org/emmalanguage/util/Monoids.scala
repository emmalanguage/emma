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
import cats.implicits._
import shapeless._
import shapeless.labelled._

import scala.collection.SortedSet
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds

/** Missing instances of [[cats.Monoid]]. */
object Monoids {

  /** Conjunctive boolean monoid. */
  val conj: Monoid[Boolean] = new Monoid[Boolean] {
    override def empty: Boolean = true
    override def combine(x: Boolean, y: Boolean): Boolean = x && y
  }

  /** Disjunctive boolean monoid. */
  val disj: Monoid[Boolean] = new Monoid[Boolean] {
    override def empty: Boolean = false
    override def combine(x: Boolean, y: Boolean): Boolean = x || y
  }

  /** Monoid for [[shapeless]] fields with an underlying monoid. */
  implicit def kv[K, V](implicit M: Monoid[V]): Monoid[K ->> V] = new Monoid[K ->> V] {
    override lazy val empty: FieldType[K, V] = field[K](M.empty)
    override def combine(x: K ->> V, y: K ->> V): FieldType[K, V] =
      field[K](M.combine(x, y))
  }

  /** [[shapeless.HNil]] is an empty monoid. */
  implicit val hnil: Monoid[HNil] = new Monoid[HNil] {
    override def empty: HNil = HNil
    override def combine(x: HNil, y: HNil): HNil = HNil
  }

  /** [[shapeless.HList]] is a generic product. */
  implicit def hcons[H, T <: HList](implicit Mh: Monoid[H], Mt: Monoid[T]): Monoid[H :: T] =
    new Monoid[H :: T] {
      override lazy val empty: H :: T = Mh.empty :: Mt.empty
      override def combine(x: H :: T, y: H :: T): H :: T =
        Mh.combine(x.head, y.head) :: Mt.combine(x.tail, y.tail)
    }

  /** Trivial monoid with bias to the left. */
  def left[A](zero: => A): Monoid[A] = new Monoid[A] {
    override lazy val empty: A = zero
    override def combine(x: A, y: A): A = if (x == empty) y else x
  }

  /** Trivial monoid with bias to the right. */
  def right[A](zero: => A): Monoid[A] = new Monoid[A] {
    override lazy val empty: A = zero
    override def combine(x: A, y: A): A = if (y == empty) x else y
  }

  /** Monoid for maps that overwrites previous values. */
  implicit def overwrite[K, V]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    override def empty: Map[K, V] = Map.empty
    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] = x ++ y
  }

  /** Monoid for maps that merges previous values via an underlying monoid. */
  def merge[K, V](implicit M: Monoid[V]): Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    override lazy val empty: Map[K, V] = Map.empty.withDefaultValue(M.empty)
    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] =
      y.foldLeft(x.withDefaultValue(M.empty)) {
        case (map, (k, v)) => map + (k -> (map(k) |+| v))
      }
  }

  /** Monoid for collections that preserves only the last `n` elements. */
  def sliding[Col[x] <: Iterable[x], El](n: Int)
    (implicit Col: CanBuildFrom[Nothing, El, Col[El]])
    : Monoid[Col[El]] = new Monoid[Col[El]] {
      override lazy val empty: Col[El] = Col().result()
      override def combine(x: Col[El], y: Col[El]): Col[El] =
        x ++ y takeRight n to Col
    }

  /** Monoid for sorted sets. */
  implicit def sortedSet[A: Ordering]: Monoid[SortedSet[A]] = new Monoid[SortedSet[A]] {
    override def empty: SortedSet[A] = SortedSet.empty
    override def combine(x: SortedSet[A], y: SortedSet[A]): SortedSet[A] = x | y
  }

  /** Reverses an existing monoid (i.e. applying it right to left). */
  def reverse[A](implicit m: Monoid[A]): Monoid[A] = new Monoid[A] {
    override def empty: A = m.empty
    override def combine(x: A, y: A): A = m.combine(y, x)
  }
}
