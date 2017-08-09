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
package lib.ml

import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector

object testutil {

  def solve(instances: Seq[LDPoint[Int, Double]]): Array[Double] = {
    val Xv: Seq[DenseMatrix[Double]] = instances.map(x => DenseMatrix(x.pos.values))

    val X: DenseMatrix[Double] = DenseMatrix.vertcat(Xv:_*)
    val Y: DenseVector[Double] = DenseVector(instances.map(x => x.label).toArray)

    val A: DenseMatrix[Double] = X.t * X
    val b: DenseVector[Double] = X.t * Y
    val w: DenseVector[Double] = A \ b

    w.valuesIterator.toArray
  }
}
