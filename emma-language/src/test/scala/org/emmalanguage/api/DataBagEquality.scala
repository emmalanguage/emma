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

import scala.language.higherKinds
import scala.reflect.ClassTag

trait DataBagEquality {

  implicit def equality[M[_] <: DataBag[_], A] = new org.scalactic.Equality[M[A]] {

    val bagTag: ClassTag[M[_]] = implicitly[ClassTag[M[_]]]

    override def areEqual(lhs: M[A], any: Any): Boolean = any match {
      case rhs: DataBag[_] =>
        val lhsVals = lhs.collect()
        val rhsVals = rhs.collect()

        val lhsXtra = lhsVals diff rhsVals
        val rhsXtra = rhsVals diff lhsVals

        lhsXtra.isEmpty && rhsXtra.isEmpty
      case _ =>
        false
    }
  }
}

