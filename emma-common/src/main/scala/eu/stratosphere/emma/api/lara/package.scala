package eu.stratosphere.emma.api

import spire.math._

import scala.reflect.ClassTag

package object lara {

  trait Matrix[A] {

    val numRows: Int
    val numCols: Int

    val cRange = 0 until numCols
    val rRange = 0 until numRows

    val transposed: Boolean

    private[lara] def toArray: Array[A]

    //////////////////////////////////////////
    // pointwise M o scalar
    //////////////////////////////////////////

    def +(that: A): Matrix[A]

    def -(that: A): Matrix[A]

    def *(that: A): Matrix[A]

    def /(that: A): Matrix[A]

    //////////////////////////////////////////
    // pointwise M o vector
    //////////////////////////////////////////

    def +(that: Vector[A]): Matrix[A]

    def -(that: Vector[A]): Matrix[A]

    def *(that: Vector[A]): Matrix[A]

    def /(that: Vector[A]): Matrix[A]

    //////////////////////////////////////////
    // pointwise M o M
    //////////////////////////////////////////

    def +(that: Matrix[A]): Matrix[A]

    def -(that: Matrix[A]): Matrix[A]

    def *(that: Matrix[A]): Matrix[A]

    def /(that: Matrix[A]): Matrix[A]

    def %*%(that: Matrix[A]): Matrix[A]

    //////////////////////////////////////////
    // M operation
    //////////////////////////////////////////

    def inv(): Matrix[A]

    def diag(): Vector[A]

    def transpose(): Matrix[A]

    def row(rowIndex: Int): Vector[A]

    def column(colIndex: Int): Vector[A]

    def rows(): Traversable[Vector[A]]

    def cols(): Traversable[Vector[A]]

    //////////////////////////////////////////
    // helper
    //////////////////////////////////////////

    private[emma] def get(i: Int, j: Int): A

    private[emma] def map[B: Numeric : ClassTag](f: (A) => B): Matrix[B]
  }

  trait Vector[A] {

    val length: Int

    val Range = 0 until length

    val rowVector: Boolean

    private[api] val intern: Array[A]

    //////////////////////////////////////////
    // pointwise vector o scalar
    //////////////////////////////////////////

    def +(that: A): Vector[A]

    def -(that: A): Vector[A]

    def *(that: A): Vector[A]

    def /(that: A): Vector[A]

    //////////////////////////////////////////
    // pointwise vector o vector
    //////////////////////////////////////////

    def +(that: Vector[A]): Vector[A]

    def -(that: Vector[A]): Vector[A]

    def *(that: Vector[A]): Vector[A]

    def /(that: Vector[A]): Vector[A]

    /**
      * Inner / Dot product / row x column vector.
      *
      * @param that the row vector
      * @return the inner product
      */
    def dot(that: Vector[A]): A

    /**
      * Outer product / column x row vector.
      *
      * @param that the column vector
      * @return the outer product [[Matrix]]
      */
    def %*%(that: Vector[A]): Matrix[A]

    /**
      * Row vector x matrix.
      *
      * @param that the matrix
      * @return the resulting row vector
      */
    def %*%(that: Matrix[A]): Vector[A]

    //////////////////////////////////////////
    // vector operation
    //////////////////////////////////////////

    def transpose(): Vector[A]

    /**
      * Returns a [[Matrix]] with the vector elements as diagonal.
      *
      * @return a [[Matrix]] with the vector elements as diagonal.
      */
    def diag(): Matrix[A]

    def aggregate(f: (A, A) => A): A

    def fold[B](zero: B, to: A => B, f: (B, B) => B): B

    def map[B: Numeric : ClassTag](f: (A) => B): Vector[B]

    //////////////////////////////////////////
    // helper
    //////////////////////////////////////////

    private[emma] def get(i: Int): A
  }

  object Matrix {

    //////////////////////////////////////////
    // Factories
    //////////////////////////////////////////

    def apply[A: Numeric : ClassTag](): Matrix[A] = new DenseMatrix[A](0, 0, Array.empty[A])

    def apply[A: Numeric : ClassTag](rows: Int, cols: Int, vals: Array[A]): Matrix[A] = {
      new DenseMatrix[A](rows, cols, vals)
    }

    //////////////////////////////////////////
    // Generators
    // TODO: THIS SHOULD BE REPLACED BY CTORS USING EITHER DENSE OR SPARSE IMPLEMENTATION
    //////////////////////////////////////////

    // TODO: make version with sparsity hint
    def fill[A: Numeric : ClassTag](rows: Int, cols: Int)(gen: (Int, Int) => A): Matrix[A] = {
      require(rows * cols < Int.MaxValue)
      val array = new Array[A](rows * cols)
      for (i <- 0 until rows; j <- 0 until cols) {
        array((i * cols) + j) = gen(i, j)
      }
      new DenseMatrix[A](rows, cols, array)
    }

    def zeros[A: Numeric : ClassTag](rows: Int, cols: Int): Matrix[A] = {
      new DenseMatrix[A](rows, cols, Array.fill(rows * cols)(implicitly[Numeric[A]].zero))
    }

    def ones[A: Numeric : ClassTag](rows: Int, cols: Int): Matrix[A] = {
      new DenseMatrix[A](rows, cols, Array.fill(rows * cols)(implicitly[Numeric[A]].one))
    }

    def eye[A: Numeric : ClassTag](rows: Int): Matrix[A] = {
      Matrix.fill(rows, rows)((i, j) => if (i == j) implicitly[Numeric[A]].one else implicitly[Numeric[A]].zero)
    }

    //////////////////////////////////////////
    // TRANSFORMATIONS
    //////////////////////////////////////////

    def toMatrix[A: Numeric : ClassTag](bag: DataBag[(Int, Int, A)])(rows: Int, cols: Int): Matrix[A] = {
      val indexer = Utils.indexed(rows, cols)
      val values = Array.ofDim[A](rows * cols)
      for (e <- bag.vals) {
        values(indexer(e._1, e._2)) = e._3
      }
      new DenseMatrix[A](rows, cols, values)
    }

    def toBag[A: Numeric](matrix: Matrix[A]): DataBag[A] = {
      DataBag(matrix.toArray)
    }
  }

  object Vector {

    //////////////////////////////////////////
    // Factories
    //////////////////////////////////////////

    def apply[A: Numeric : ClassTag](): Vector[A] = new DenseVector[A](0, Array.empty[A])

    def apply[A: Numeric : ClassTag](values: A*): Vector[A] = new DenseVector[A](values.size, values.toArray[A])

    def apply[A: Numeric : ClassTag](values: Array[A], isRowVector: Boolean = false): Vector[A] = {
      new DenseVector[A](values.length, values)
    }

    //////////////////////////////////////////
    // Generators
    // TODO: THIS SHOULD BE REPLACED BY CTORS USING EITHER DENSE OR SPARSE IMPLEMENTATION
    //////////////////////////////////////////

    def fill[A: Numeric : ClassTag](size: Int, isRowVector: Boolean = false)(gen: (Int) => A): Vector[A] = {
      val values = new Array[A](size)
      for (i <- 0 until size) {
        values(i) = gen(i)
      }
      new DenseVector[A](size, values, rowVector = isRowVector)
    }

    def zeros[A: Numeric : ClassTag](size: Int, isRowVector: Boolean = false): Vector[A] = {
      new DenseVector[A](size, Array.fill(size)(implicitly[Numeric[A]].zero))
    }

    def ones[A: Numeric : ClassTag](size: Int, isRowVector: Boolean = false): Vector[A] = {
      new DenseVector[A](size, Array.fill(size)(implicitly[Numeric[A]].one))
    }
  }

}
