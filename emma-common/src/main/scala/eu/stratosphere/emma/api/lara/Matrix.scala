package eu.stratosphere.emma.api.lara

import spire.math.Numeric

import scala.reflect.ClassTag
import scala.util.Random

trait Matrix[A] {

  val numRows: Int
  val numCols: Int

  val cRange = 0 until numCols
  val rRange = 0 until numRows

  val transposed: Boolean

  private[emma] def toArray: Array[A]

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

  //////////////////////////////////////////
  // M x M -> M and  M x V -> V
  //////////////////////////////////////////

  def %*%(that: Matrix[A]): Matrix[A]

  def %*%(that: Vector[A]): Vector[A]

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

  def elements[B](z: B)(s: A => B, p: (B, B) => B): B

  private[emma] def map[B: Numeric : ClassTag](f: (A) => B): Matrix[B]

  private[emma] def plus(other: Matrix[A]): Matrix[A]

  //////////////////////////////////////////
  // helper
  //////////////////////////////////////////

  private[emma] def get(i: Int, j: Int): A

  private[emma] def getOpt(i: Int, j: Int): Option[A]
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

  def rand[A: Numeric : ClassTag](rows: Int, cols: Int): Matrix[A] = {
    Matrix.fill(rows, cols)((i, j) => implicitly[Numeric[A]].fromDouble(Random.nextDouble()))
  }
}