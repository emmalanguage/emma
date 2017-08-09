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
package lib

import org.apache.spark.ml.{linalg => spark}

package object linalg extends MathUtil {
  type Vector = spark.Vector
  type DVector = spark.DenseVector
  type SVector = spark.SparseVector

  def dense(values: Array[Double]): DVector =
    spark.Vectors.dense(values).asInstanceOf[DVector]

  def zeros(size: Int): DVector =
    spark.Vectors.zeros(size).asInstanceOf[DVector]

  def sparse(size: Int, indices: Array[Int], values: Array[Double]): SVector =
    spark.Vectors.sparse(size, indices, values).asInstanceOf[SVector]

  def sparse(size: Int, elements: Seq[(Int, Double)]): SVector =
    spark.Vectors.sparse(size, elements).asInstanceOf[SVector]

  def sqdist(x: Vector, y: Vector): Double =
    spark.Vectors.sqdist(x, y)

  def norm(vector: Vector, p: Double): Double =
    spark.Vectors.norm(vector, p)

  def sum(x: DVector): Double = x.values.sum

  implicit class DVectorOps(val x: DVector) extends AnyVal {
    def +=(y: DVector): Unit =
      BLAS.axpy(1.0, y, x)

    def +=(y: Double): Unit = {
      var i = 0
      val N = x.size
      val r = x.values
      while (i < N) {
        r(i) = x(i) + y
        i += 1
      }
    }

    def -=(y: DVector): Unit =
      BLAS.axpy(-1.0, y, x)

    def -=(y: Double): Unit = {
      var i = 0
      val N = x.size
      val r = x.values
      while (i < N) {
        r(i) = x(i) - y
        i += 1
      }
    }

    def *=(y: DVector): Unit = {
      val xs = x.size
      val ys = y.size
      require(xs == ys, s"Vectors must be the same length. Was: $xs, $ys")
      var i = 0
      val r = x.values
      while (i < xs) {
        r(i) = x(i) * y(i)
        i += 1
      }
    }

    def *=(a: Double): Unit =
      BLAS.scal(a, x)

    def /=(y: Double): Unit = {
      var i = 0
      val N = x.size
      val r = x.values
      while (i < N) {
        r(i) = x(i) / y
        i += 1
      }
    }

    def /=(y: DVector): Unit = {
      val xs = x.size
      val ys = y.size
      require(xs == ys, s"Vectors must be the same length. Was: $xs, $ys")
      var i = 0
      val r = x.values
      while (i < xs) {
        r(i) = x(i) / y(i)
        i += 1
      }
    }

    // non-in-place operations

    def +(y: DVector): DVector = {
      val z = y.copy
      BLAS.axpy(1.0, x, z)
      z
    }

    def +(y: Double): DVector = {
      var i = 0
      val N = x.size
      val r = Array.ofDim[Double](N)
      while (i < N) {
        r(i) = x(i) + y
        i += 1
      }
      dense(r)
    }

    def -(y: DVector): DVector = {
      val z = x.copy
      BLAS.axpy(-1.0, y, z)
      z
    }

    def -(y: Double): DVector = {
      var i = 0
      val N = x.size
      val r = Array.ofDim[Double](N)
      while (i < N) {
        r(i) = x(i) - y
        i += 1
      }
      dense(r)
    }

    def *(y: DVector): DVector = {
      val xs = x.size
      val ys = y.size
      require(xs == ys, s"Vectors must be the same length. Was: $xs, $ys")
      var i = 0
      val r = Array.ofDim[Double](xs)
      while (i < xs) {
        r(i) = x(i) * y(i)
        i += 1
      }
      dense(r)
    }

    def *(a: Double): DVector = {
      val y = x.copy
      BLAS.scal(a, y)
      y
    }

    def /(y: DVector): DVector = {
      val xs = x.size
      val ys = y.size
      require(xs == ys, s"Vectors must be the same length. Was: $xs, $ys")
      var i = 0
      val r = Array.ofDim[Double](xs)
      while (i < xs) {
        r(i) = x(i) / y(i)
        i += 1
      }
      dense(r)
    }

    def /(y: Double): DVector = {
      var i = 0
      val N = x.size
      val r = Array.ofDim[Double](N)
      while (i < N) {
        r(i) = x(i) / y
        i += 1
      }
      dense(r)
    }

    def dot(y: DVector): Double =
      BLAS.dot(x, y)

    def max(y: DVector): DVector = {
      var i = 0
      val N = x.size
      val r = Array.ofDim[Double](N)
      while (i < N) {
        r(i) = Math.max(x(i), y(i))
        i += 1
      }
      dense(r)
    }

    def min(y: DVector): DVector = {
      var i = 0
      val N = x.size
      val r = Array.ofDim[Double](N)
      while (i < N) {
        r(i) = Math.min(x(i), y(i))
        i += 1
      }
      dense(r)
    }
  }

}

