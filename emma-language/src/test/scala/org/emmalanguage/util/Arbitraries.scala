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

import shapeless._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

/** Missing instances of [[org.scalacheck.Arbitrary]]. */
trait Arbitraries {

  /** Arbitrary empty product. */
  implicit val hNilArb: Arbitrary[HNil] =
    Arbitrary(Gen.const(HNil))

  /** Arbitrary generic products. */
  implicit def hConsArb[H, T <: HList](implicit H: Arbitrary[H], T: Arbitrary[T])
    : Arbitrary[H :: T] = Arbitrary(for {
      h <- H.arbitrary
      t <- T.arbitrary
    } yield h :: t)
}

/** Missing instances of [[org.scalacheck.Arbitrary]]. */
object Arbitraries extends Arbitraries
