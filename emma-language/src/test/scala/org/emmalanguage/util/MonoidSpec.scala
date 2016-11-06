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
import shapeless._
import shapeless.syntax.singleton._

import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

import scala.collection.SortedSet

@RunWith(classOf[JUnitRunner])
class MonoidSpec extends FreeSpec with Checkers with Equivalences with Arbitraries {

  import Monoids._

  def arb[A](implicit Arb: Arbitrary[A]): Gen[A] =
    Arb.arbitrary

  "Monoid laws" - {

    "forgetful" - {

      "with left bias" in {
        check(GroupLaws[Int].monoid(left(0)).all)
      }

      "with right bias" in {
        check(GroupLaws[String].monoid(right("")).all)
      }
    }
    
    "for booleans" - {
      
      "conjunctive" in {
        check(GroupLaws[Boolean].monoid(conj).all)
      }
      
      "disjunctive" in {
        check(GroupLaws[Boolean].monoid(disj).all)
      }
    }

    "for maps" - {

      "with overwriting" in {
        check(GroupLaws[Map[Char, Int]].monoid(overwrite).all)
      }

      "with merging" in {
        check(GroupLaws[Map[Char, Int]].monoid(merge).all)
      }
    }

    "for generic products" in {
      check(GroupLaws[HNil].monoid.all)
      check(GroupLaws[Int :: String :: HNil].monoid.all)
    }

    "for labelled fields" in {
      val key = "key".witness
      check(GroupLaws[key.T ->> Int].monoid.all)
      check(GroupLaws[key.T ->> String].monoid.all)
    }

    "for sliding collections" in {
      val n = 10
      implicit val vecOfN = Arbitrary(Gen.containerOfN[Vector, Int](n, arb[Int]))
      implicit val setOfN = Arbitrary(Gen.containerOfN[Set, String](n, arb[String]))
      check(GroupLaws[Vector[Int]].monoid(sliding(n)).all)
      check(GroupLaws[Set[String]].monoid(sliding(n)).all)
    }

    "for sorted sets" in {
      check(GroupLaws[SortedSet[Int]].monoid.all)
      check(GroupLaws[SortedSet[String]].monoid.all)
    }

    "for reversed monoids (right to left)" in {
      check(GroupLaws[String].monoid(reverse).all)
      check(GroupLaws[List[Double]].monoid(reverse).all)
    }
  }
}
