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

//------------------------------------------------------------------------------
// Algebras for DVector-based statistics
//------------------------------------------------------------------------------

// Do not use this directly, the `stat` object contains
// wrapper methods delegating to each algebra.

object salg {

  case class Sum(D: Int) extends alg.Alg[DVector, DVector] {
    val zero = zeros(D)
    val init = identity[DVector] _
    val plus = (x: DVector, y: DVector) => x + y
  }

  case class Min(D: Int) extends alg.Alg[DVector, DVector] {
    val zero = dense(Array.tabulate(D)(Function.const(Double.MaxValue)))
    val init = identity[DVector] _
    val plus = (x: DVector, y: DVector) => x min y
  }

  case class Max(D: Int) extends alg.Alg[DVector, DVector] {
    val zero = dense(Array.tabulate(D)(Function.const(Double.MinValue)))
    val init = identity[DVector] _
    val plus = (x: DVector, y: DVector) => x max y
  }

}

