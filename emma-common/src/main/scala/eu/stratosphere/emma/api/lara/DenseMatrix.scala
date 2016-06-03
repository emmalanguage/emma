package eu.stratosphere.emma.api.lara

import eu.stratosphere.emma.api.linalg.ir.{VIdx, Idx}
import spire.implicits._
import spire.math._

import scala.reflect.ClassTag

/**
  * Row-major dense matrix representation.
  *
  * Values are stored in a one-dimensonal array (row-major).
  * Matrix M:
  *
  * 1, 2, 3, 4
  * 5, 6, 7, 8
  * 9, 10, 11, 12
  *
  * would be stored as [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].
  *
  * @param numRows number of rows
  * @param numCols number of columns
  * @param values the acutal values
  * @param transposed is the matrix transposed (default: false)
  * @tparam A the type of the values in the matrix
  */
private[emma] class DenseMatrix[A: Numeric : ClassTag](override val numRows: Int,
                                                       override val numCols: Int,
                                                       val values: Array[A],
                                                       override val transposed: Boolean = false) extends Matrix[A] {

  self =>

  override
  private[emma] def toArray: Array[A] = {
    if (!transposed) {
      values
    } else {
      val arr = Array.ofDim[A](numRows * numCols)
      for (i <- rRange; j <- cRange) {
        arr(i * numCols + j) = get(i, j)
      }
      arr
    }
  }

  //////////////////////////////////////////
  // pointwise M o scalar
  //////////////////////////////////////////

  override
  def +(that: A): Matrix[A] = mapScalar(that, _ + _)

  override
  def -(that: A): Matrix[A] = mapScalar(that, _ - _)

  override
  def *(that: A): Matrix[A] = mapScalar(that, _ * _)

  override
  def /(that: A): Matrix[A] = mapScalar(that, _ / _)

  //////////////////////////////////////////
  // pointwise M o vector
  //////////////////////////////////////////

  override
  def +(that: Vector[A]): Matrix[A] = mapVector(that, _ + _)

  override
  def -(that: Vector[A]): Matrix[A] = mapVector(that, _ - _)

  override
  def *(that: Vector[A]): Matrix[A] = mapVector(that, _ * _)

  override
  def /(that: Vector[A]): Matrix[A] = mapVector(that, _ / _)

  //////////////////////////////////////////
  // pointwise M o M
  //////////////////////////////////////////

  override
  def +(that: Matrix[A]): Matrix[A] = mapMatrix(that, _ + _)

  override
  def -(that: Matrix[A]): Matrix[A] = mapMatrix(that, _ - _)

  override
  def *(that: Matrix[A]): Matrix[A] = mapMatrix(that, _ * _)

  override
  def /(that: Matrix[A]): Matrix[A] = mapMatrix(that, _ / _)

  override
  def %*%(that: Matrix[A]): Matrix[A] = {
    require(numCols == that.numRows)
    // TODO: use blas, check for sparse input
    val values: Array[A] = new Array[A](numRows * that.numCols)
    for (i <- rRange; j <- that.cRange) {
      values(i * that.numCols + j) = row(i) dot that.column(j)
    }
    new DenseMatrix[A](numRows, that.numCols, values)
  }

  override
  def %*%(that: Vector[A]): Vector[A] = {
    require(numCols == that.length)
    val values: Array[A] = new Array[A](numRows)
    for (i <- rRange) {
      values(i) = row(i) dot that
    }
    new DenseVector[A](numRows, values)
  }

  //////////////////////////////////////////
  // M operation
  //////////////////////////////////////////

  override
  def diag(): Vector[A] = {
    require(numRows == numCols, s"matrix is not quadratic")

    val values: Array[A] = new Array[A](numRows)
    for (i <- rRange) {
      values(i) = get(i, i)
    }
    new DenseVector[A](numRows, values, rowVector = false)
  }

  override
  def transpose(): Matrix[A] = {
    new DenseMatrix[A](numCols, numRows, values, !transposed)
  }

  override
  def row(rowIndex: Int): Vector[A] = {
    require(rowIndex < numRows)
    Vector.fill[A](numCols, isRowVector = true)(i => get(rowIndex, i))
  }

  override
  def column(colIndex: Int): Vector[A] = {
    require(colIndex < numCols)
    Vector.fill[A](numRows, isRowVector = false)(i => get(i, colIndex))
  }

  override
  def rows[B: Numeric : ClassTag](f: Vector[A] => B): Vector[B] = {
    val arr = Array.ofDim[B](numRows)
    for (i <- rRange) {
      arr(i) = f(row(i))
    }
    Vector[B](arr, isRowVector = true)
  }

  override
  def rows[B: Numeric : ClassTag](f: Vector[A] => Vector[B]): Matrix[B] = {
    val arr = Array.ofDim[B](numRows * numCols)
    for (i <- rRange) {
      val newVec = f(row(i))
      require(newVec.length == numCols)
      for (j <- newVec.Range) {
        arr(index(i, j)) = newVec.get(j)
      }
    }
    Matrix[B](numRows, numCols, arr)
  }

  override
  def indexedRows[B: Numeric : ClassTag](f: Idx[Int, Vector[A]] => B): Vector[B] = {
    val arr = Array.ofDim[B](numRows)
    for (i <- rRange) {
      arr(i) = f(VIdx(i, row(i)))
    }
    Vector[B](arr, isRowVector = true)
  }

  override
  def indexedRows[B: Numeric : ClassTag](f: Idx[Int, Vector[A]] => Vector[B]): Matrix[B] = {
    val arr = Array.ofDim[B](numRows * numCols)
    for (i <- rRange) {
      val newVec = f(VIdx(i,row(i)))
      require(newVec.length == numCols)
      for (j <- newVec.Range) {
        arr(index(i, j)) = newVec.get(j)
      }
    }
    Matrix[B](numRows, numCols, arr)
  }

  override
  def cols[B: Numeric : ClassTag](f: Vector[A] => B): Vector[B] = {
    val arr = Array.ofDim[B](numCols)
    for (j <- cRange) {
      arr(j) = f(column(j))
    }
    Vector[B](arr, isRowVector = false)
  }

  override
  def cols[B: Numeric : ClassTag](f: Vector[A] => Vector[B]): Matrix[B] = {
    val arr = Array.ofDim[B](numRows * numCols)
    for (j <- cRange) {
      val newVec = f(column(j))
      require(newVec.length == numRows)
      for (i <- newVec.Range) {
        arr(index(i, j)) = newVec.get(i)
      }
    }
    Matrix[B](numRows, numCols, arr)
  }

  override
  def indexedCols[B: Numeric : ClassTag](f: Idx[Int, Vector[A]] => B): Vector[B] = {
    val arr = Array.ofDim[B](numCols)
    for (i <- cRange) {
      arr(i) = f(VIdx(i, column(i)))
    }
    Vector[B](arr, isRowVector = false)
  }

  override
  def indexedCols[B: Numeric : ClassTag](f: Idx[Int, Vector[A]] => Vector[B]): Matrix[B] = {
    val arr = Array.ofDim[B](numRows * numCols)
    for (j <- cRange) {
      val newVec = f(VIdx(j,column(j)))
      require(newVec.length == numRows)
      for (i <- newVec.Range) {
        arr(index(i, j)) = newVec.get(i)
      }
    }
    Matrix[B](numRows, numCols, arr)
  }

  override
  def elements[B](z: B)(s: A => B, p: (B, B) => B): B = {
    values.foldLeft(z)((acc, x) => p(acc, s(x)))
  }

  override
  private[emma] def map[B: Numeric : ClassTag](f: (A) => B): Matrix[B] = {
    val res: Array[B] = values.map(f(_))
    new DenseMatrix(numRows, numCols, res)
  }

  override
  private[emma] def plus(other: Matrix[A]): Matrix[A] = {
    val rMax: Int = Math.max(numRows, other.numRows)
    val cMax: Int = Math.max(numCols, other.numCols)
    Matrix.fill(rMax, cMax) { (i, j) =>
      other.getOpt(i, j).getOrElse {
        self.getOpt(i, j).getOrElse(implicitly[Numeric[A]].zero)
      }
    }
  }

  //////////////////////////////////////////
  // helper
  //////////////////////////////////////////

  private[lara] def mapScalar(scalar: A, f: (A, A) => A): DenseMatrix[A] = {
    val ar: Array[A] = values.map(f(_, scalar))
    new DenseMatrix(numRows, numCols, ar)
  }

  private[lara] def mapMatrix(that: Matrix[A], f: (A, A) => A): DenseMatrix[A] = {
    require(numRows == that.numRows)
    require(numCols == that.numCols)

    val mapped = new Array[A](values.length)
    for (i <- rRange; j <- cRange) {
      mapped(index(i, j)) = f(get(i, j), that.get(i, j))
    }
    new DenseMatrix[A](numRows, numCols, mapped)
  }

  private[lara] def mapVector(that: Vector[A], f: (A, A) => A): DenseMatrix[A] = {
    val m = that match {
      case v if v.rowVector =>
        require(numCols == v.length)
        Matrix.fill(numRows, numCols)((i, j) => v.get(j))

      case v =>
        require(numRows == v.length)
        Matrix.fill(numRows, numCols)((i, j) => v.get(i))
    }
    mapMatrix(m, f)
  }

  override
  private[emma] def get(i: Int, j: Int): A = {
    values(index(i, j))
  }

  private[emma] def getOpt(i: Int, j: Int): Option[A] = {
    if (i < numRows && j < numCols) {
      Some(get(i, j))
    } else {
      None
    }
  }

  private[emma] def set(i: Int, j: Int)(value: A): Unit = {
    values(index(i, j)) = value
  }

  private[emma] def index(i: Int, j: Int): Int = {
    require(i < numRows, s"supplied row index $i > $numRows")
    require(j < numCols, s"supplied col index $j > $numCols")
    if (!transposed) i * numCols + j else j * numRows + i
  }

  //////////////////////////////////////////

  // just for short comparison
  lazy private val hash: Int = {
    var result = 17
    result = if (transposed) 37 * result else 37 * result + 1
    result = 37 * result + (numRows ^ (numRows >>> 32))
    result = 37 * result + (numCols ^ (numCols >>> 32))
    result
  }

  override
  def hashCode(): Int = hash

  override
  def equals(that: Any): Boolean = {
    def compare(other: Matrix[A]): Boolean = {
      for (i <- rRange; j <- cRange) {
        if (get(i, j) != other.get(i, j)) {
          return false
        }
      }
      true
    }
    that match {
      case o: Matrix[A] =>
        o.hashCode() == hashCode() &&
        o.transposed == transposed &&
        o.numRows == numRows &&
        o.numCols == numCols &&
        compare(o)

      case _ => false
    }
  }

  override
  def toString: String = {
    val builder = StringBuilder.newBuilder
    for (i <- rRange) {
      builder ++= (for (j <- cRange) yield get(i, j)).mkString("", ", ", "\n")
    }
    builder.result()
  }
}