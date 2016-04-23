package eu.stratosphere.emma.api.lara

import spire.math.Numeric

import scala.collection.immutable.NumericRange
import scala.reflect.ClassTag
import scala.util.Random

trait Vector[A] {

  val length: Int

  val Range = 0 until length

  val rowVector: Boolean

  private[emma] val toArray: Array[A]

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

  def fold[B](z: B)(s: A => B, p: (B, B) => B): B

  def map[B: Numeric : ClassTag](f: (A) => B): Vector[B]

  private[emma] def plus(other: Vector[A]): Vector[A]

  //////////////////////////////////////////
  // helper
  //////////////////////////////////////////

  private[emma] def get(i: Int): A

  private[emma] def getOpt(i: Int): Option[A]
}


object Vector {

  //////////////////////////////////////////
  // Factories
  //////////////////////////////////////////

  def apply[A: Numeric : ClassTag](): Vector[A] = new DenseVector[A](0, Array.empty[A])

  def apply[A: Numeric : ClassTag](values: Array[A], isRowVector: Boolean = false): Vector[A] = {
    new DenseVector[A](values.length, values, rowVector = isRowVector)
  }

  def apply[A: Numeric : ClassTag](range: NumericRange[A]): Vector[A] = {
    new DenseVector[A](range.length, range.toArray)
  }

  def apply[_ <: Int](range: Range): Vector[Int] = {
    new DenseVector[Int](range.length, range.toArray)
  }

  def apply[A: Numeric : ClassTag](range: Range.Partial[Double, NumericRange[A]]): Vector[A] = {
    apply[A](range.by(1.0))
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
    new DenseVector[A](size, Array.fill(size)(implicitly[Numeric[A]].zero), rowVector = isRowVector)
  }

  def zerosLike[A: Numeric : ClassTag](that: Vector[A]): Vector[A] = {
    Vector.zeros[A](that.length)
  }

  def ones[A: Numeric : ClassTag](size: Int, isRowVector: Boolean = false): Vector[A] = {
    new DenseVector[A](size, Array.fill(size)(implicitly[Numeric[A]].one), rowVector = isRowVector)
  }

  def rand[A: Numeric : ClassTag](size: Int, isRowVector: Boolean = false): Vector[A] = {
    val rng = new Random()
    new DenseVector[A](
      size,
      Array.fill(size)(implicitly[Numeric[A]].fromDouble(rng.nextDouble())),
      rowVector = isRowVector
    )
  }

  def rand[A: Numeric : ClassTag](size: Int, isRowVector: Boolean, seed: Long): Vector[A] = {
    val rng = new Random(seed)
    new DenseVector[A](size,
      Array.fill(size)(implicitly[Numeric[A]].fromDouble(rng.nextDouble())),
      rowVector = isRowVector
    )
  }
}