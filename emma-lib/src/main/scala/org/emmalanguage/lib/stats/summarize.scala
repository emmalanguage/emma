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
package lib.stats

import api._
import lib.linalg._

@emma.lib
object summarize {
  def apply[B: Meta](
    s: DataBag[DVector] => B
  )(xs: DataBag[DVector]): B =
    s(xs)

  def apply[B1: Meta, B2: Meta](
    s1: DataBag[DVector] => B1,
    s2: DataBag[DVector] => B2
  )(xs: DataBag[DVector]): (B1, B2) = (
    s1(xs), s2(xs)
  )

  def apply[B1: Meta, B2: Meta, B3: Meta](
    s1: DataBag[DVector] => B1,
    s2: DataBag[DVector] => B2,
    s3: DataBag[DVector] => B3
  )(xs: DataBag[DVector]): (B1, B2, B3) = (
    s1(xs), s2(xs), s3(xs)
  )

  def apply[B1: Meta, B2: Meta, B3: Meta, B4: Meta](
    s1: DataBag[DVector] => B1,
    s2: DataBag[DVector] => B2,
    s3: DataBag[DVector] => B3,
    s4: DataBag[DVector] => B4
  )(xs: DataBag[DVector]): (B1, B2, B3, B4) = (
    s1(xs), s2(xs), s3(xs), s4(xs)
  )

  def apply[B1: Meta, B2: Meta, B3: Meta, B4: Meta, B5: Meta](
    s1: DataBag[DVector] => B1,
    s2: DataBag[DVector] => B2,
    s3: DataBag[DVector] => B3,
    s4: DataBag[DVector] => B4,
    s5: DataBag[DVector] => B5
  )(xs: DataBag[DVector]): (B1, B2, B3, B4, B5) = (
    s1(xs), s2(xs), s3(xs), s4(xs), s5(xs)
  )

  def apply[B1: Meta, B2: Meta, B3: Meta, B4: Meta, B5: Meta, B6: Meta](
    s1: DataBag[DVector] => B1,
    s2: DataBag[DVector] => B2,
    s3: DataBag[DVector] => B3,
    s4: DataBag[DVector] => B4,
    s5: DataBag[DVector] => B5,
    s6: DataBag[DVector] => B6
  )(xs: DataBag[DVector]): (B1, B2, B3, B4, B5, B6) = (
    s1(xs), s2(xs), s3(xs), s4(xs), s5(xs), s6(xs)
  )
}
