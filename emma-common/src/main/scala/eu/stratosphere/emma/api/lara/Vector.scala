package eu.stratosphere.emma.api.lara

import spire.math.Numeric

import scala.reflect.ClassTag

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

  def ones[A: Numeric : ClassTag](size: Int, isRowVector: Boolean = false): Vector[A] = {
    new DenseVector[A](size, Array.fill(size)(implicitly[Numeric[A]].one), rowVector = isRowVector)
  }
}