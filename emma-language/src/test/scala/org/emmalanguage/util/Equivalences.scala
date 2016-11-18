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

import cats.Eq
import shapeless._

import scala.collection.SortedSet

/** Missing instances of [[cats.Eq]]. */
trait Equivalences {

  /** Equivalence of the empty product. */
  implicit val hNilEq: Eq[HNil] = new Eq[HNil] {
    def eqv(x: HNil, y: HNil) = true
  }

  /** Equivalence of generic products. */
  implicit def hConsEq[H, T <: HList](implicit H: Eq[H], T: Eq[T])
    : Eq[H :: T] = new Eq[H :: T] {
      def eqv(x: H :: T, y: H :: T) =
        H.eqv(x.head, y.head) && T.eqv(x.tail, y.tail)
    }

  /** Equivalence of sorted sets. */
  implicit def sortedSetEq[A](implicit A: Eq[A]): Eq[SortedSet[A]] = new Eq[SortedSet[A]] {
    def eqv(x: SortedSet[A], y: SortedSet[A]) =
      x.size == y.size && x.zip(y).forall((A.eqv _).tupled)
  }
}

/** Missing instances of [[cats.Eq]]. */
object Equivalences extends Equivalences
