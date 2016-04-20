package eu.stratosphere.emma.api.lara

import spire.implicits._
import spire.math._

import scala.reflect.ClassTag

private[emma] class DenseVector[A: Numeric : ClassTag](
                                                        override val length: Int,
                                                        val values: Array[A],
                                                        override val rowVector: Boolean = false) extends Vector[A] {

  self =>

  private[emma] override val toArray = values

  //////////////////////////////////////////
  // pointwise M o scalar
  //////////////////////////////////////////

  override
  def +(that: A): Vector[A] = mapScalar(that, _ + _)

  override
  def -(that: A): Vector[A] = mapScalar(that, _ - _)

  override
  def *(that: A): Vector[A] = mapScalar(that, _ * _)

  override
  def /(that: A): Vector[A] = mapScalar(that, _ / _)

  //////////////////////////////////////////
  // pointwise M o M
  //////////////////////////////////////////

  override
  def +(that: Vector[A]): Vector[A] = mapVector(that, _ + _)

  override
  def -(that: Vector[A]): Vector[A] = mapVector(that, _ - _)

  override
  def *(that: Vector[A]): Vector[A] = mapVector(that, _ * _)

  override
  def /(that: Vector[A]): Vector[A] = mapVector(that, _ / _)

  override
  def dot(that: Vector[A]): A = {
    // TODO: Should be enforce the orientation of the vectors?
    require(length == that.length)
    (for (i <- Range) yield get(i) * that.get(i)).reduce(_ + _)
  }

  override
  def %*%(that: Vector[A]): Matrix[A] = {
    // TODO: Should we enforce the orientation of the vectors?
    Matrix.fill[A](length, that.length)((i, j) => get(i) * that.get(j))
  }

  override def %*%(that: Matrix[A]): Vector[A] = {
    require(self.rowVector)
    require(self.length == that.numRows)
    Vector.fill[A](that.numCols, true)(i => self dot that.column(i))
  }

  //////////////////////////////////////////
  // vector operations
  //////////////////////////////////////////

  override
  def transpose(): Vector[A] = {
    new DenseVector(length, values, rowVector = !self.rowVector)
  }

  override
  def diag(): Matrix[A] = {
    Matrix.fill(length, length) { (i, j) =>
      if (i == j) {
        this.get(i)
      } else {
        implicitly[Numeric[A]].zero
      }
    }
  }

  override
  def aggregate(f: (A, A) => A): A = {
    values.reduce(f)
  }

  override
  def fold[B](z: B)(s: A => B, p: (B, B) => B): B = {
    values.foldLeft(z)((acc, x) => p(s(x), acc))
  }

  override
  def map[B: Numeric : ClassTag](f: (A) => B): Vector[B] = {
    new DenseVector[B](length, values.map(f(_)), rowVector)
  }

  override
  def plus(other: Vector[A]): Vector[A] = {
    val newLength = if (length > other.length) length else other.length
    Vector.fill(newLength) { i =>
      other.getOpt(i).getOrElse {
        self.getOpt(i).getOrElse(implicitly[Numeric[A]].zero)
      }
    }
  }

  //  override
  //  def flatMap[B : Numeric : ClassTag](f: (A) => Vector[B]): Vector[B] = {
  //    val flattend = values.flatMap(e => f(e).intern)
  //    new DenseVector[B](flattend.length, flattend, rowVector)
  //  }
  //
  //  override
  //  def withFilter(p: (A) => Boolean): Vector[A] = {
  //    val filteredValues = values.filter(p(_))
  //    new DenseVector[A](filteredValues.length, filteredValues, rowVector)
  //  }

  //////////////////////////////////////////
  // helper
  //////////////////////////////////////////

  override
  private[emma] def get(i: Int): A = values(i)

  override
  private[emma] def getOpt(i: Int): Option[A] = {
    if (i < length) {
      Some(get(i))
    } else {
      None
    }
  }

  private[lara] def mapScalar(scalar: A, f: (A, A) => A): Vector[A] = {
    new DenseVector[A](length, values.map(f(_, scalar)), rowVector)
  }

  private[lara] def mapVector(that: Vector[A], f: (A, A) => A): Vector[A] = {
    require(length == that.length, s"vectors have different size")

    val res: Array[A] = new Array[A](length)
    for (i <- Range) {
      res(i) = f(get(i), that.get(i))
    }
    new DenseVector[A](length, res, rowVector)
  }

  //////////////////////////////////////////

  // just for short comparison
  lazy private val hash: Int = {
    var result = 17
    result = if (rowVector) 37 * result else 37 * result + 1
    result = 37 * result + (length ^ (length >>> 32))
    result
  }

  override
  def hashCode(): Int = hash

  override
  def equals(that: Any): Boolean = {
    def compare(other: Vector[A]): Boolean = {
      for (i <- Range) {
        if (get(i) != other.get(i)) {
          return false
        }
      }
      true
    }
    that match {
      case o: Vector[A] =>
        o.hashCode() == hashCode() &&
        o.rowVector == rowVector &&
        o.length == length &&
        compare(o)

      case _ => false
    }
  }

  override
  def toString: String = {
    s"[${values.mkString(", ")}]"
  }
}
