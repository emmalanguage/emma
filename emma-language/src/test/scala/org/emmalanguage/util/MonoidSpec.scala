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

import cats.implicits._
import cats.kernel.laws.GroupLaws
import org.scalacheck._
import org.scalatest._
import org.scalatest.prop.Checkers
import shapeless._

import scala.collection.SortedSet

class MonoidSpec extends FreeSpec with Checkers with Equivalences with Arbitraries {

  import Monoids._

  def arb[A](implicit Arb: Arbitrary[A]): Gen[A] =
    Arb.arbitrary

  "Monoid laws" - {

    "forgetful" - {

      "with left bias" in {
        for ((_, prop) <- GroupLaws[Int].monoid(first(0)).all.properties) check(prop)
      }

      "with right bias" in {
        for ((_, prop) <- GroupLaws[String].monoid(last("")).all.properties) check(prop)
      }
    }

    "for booleans" - {

      "conjunctive" in {
        for ((_, prop) <- GroupLaws[Boolean].monoid(conj).all.properties) check(prop)
      }

      "disjunctive" in {
        for ((_, prop) <- GroupLaws[Boolean].monoid(disj).all.properties) check(prop)
      }
    }

    "for maps" - {

      "with overwriting" in {
        for ((_, prop) <- GroupLaws[Map[Char, Int]].monoid(overwrite).all.properties) check(prop)
      }

      "with merging" in {
        for ((_, prop) <- GroupLaws[Map[Char, Int]].monoid(merge).all.properties) check(prop)
      }
    }

    "for generic products" in {
      for ((_, prop) <- GroupLaws[HNil].monoid.all.properties) check(prop)
      for ((_, prop) <- GroupLaws[Int :: String :: HNil].monoid.all.properties) check(prop)
    }

    "for sliding collections" in {
      val n = 10
      implicit val vecOfN = Arbitrary(Gen.containerOfN[Vector, Int](n, arb[Int]))
      implicit val setOfN = Arbitrary(Gen.containerOfN[Set, String](n, arb[String]))
      for ((_, prop) <- GroupLaws[Vector[Int]].monoid(sliding(n)).all.properties) check(prop)
      for ((_, prop) <- GroupLaws[Set[String]].monoid(sliding(n)).all.properties) check(prop)
    }

    "for sorted sets" in {
      for ((_, prop) <- GroupLaws[SortedSet[Int]].monoid.all.properties) check(prop)
      for ((_, prop) <- GroupLaws[SortedSet[String]].monoid.all.properties) check(prop)
    }

    "for reversed monoids (right to left)" in {
      for ((_, prop) <- GroupLaws[String].monoid(reverse).all.properties) check(prop)
      for ((_, prop) <- GroupLaws[List[Double]].monoid(reverse).all.properties) check(prop)
    }
  }
}
