package eu.stratosphere.emma.api.lara

import spire.implicits._
import spire.math._

import scala.reflect.ClassTag

class DenseVector[A: Numeric : ClassTag](
                                          override val length: Int,
                                          val values: Array[A],
                                          override val rowVector: Boolean = false) extends Vector[A] {

  self =>

  private[api] override val intern = values

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
  def fold[B](zero: B, to: A => B, f: (B, B) => B): B = {
    values.foldLeft(zero)((b, a) => f(b, to(a)))
  }

  //////////////////////////////////////////
  // helper
  //////////////////////////////////////////

  override
  private[emma] def get(i: Int): A = values(i)

  private[lara] def mapScalar(scalar: A, f: (A, A) => A): Vector[A] = {
    new DenseVector[A](length, values.map(f(_, scalar)), rowVector)
  }

  private[lara] def mapVector(that: Vector[A], f: (A, A) => A): Vector[A] = {
    require(length == that.length, s"vectors have different size")

    val res: Array[A] = new Array[A](length)
    for (i <- Range) {
      res(i) = f(get(i), that.get(i))
    }
    new DenseVector[A](
      length,
      res,
      rowVector
    )
  }

  override
  def toString: String = {
    //    s"[${vals.map(_.value).mkString(",")}]"
    s"[${values.mkString(", ")}]"
  }

  //////////////////////////////////////////
  // Monad stuff
  //////////////////////////////////////////

  override
  def map[B: Numeric : ClassTag](f: (A) => B): Vector[B] = {
    new DenseVector[B](length, values.map(f(_)), rowVector)
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

  //  override
  //  def equals(that: Any): Boolean = {
  //    def compare(other: Vector[A]): Boolean = {
  //      for (i <- 0 until length) {
  //        if (get(i) != other.get(i))
  //          return false
  //      }
  //      true
  //    }
  //    // TODO: Should we only check for dense or also compare against sparse vectors
  //    that match {
  //      case o: Vector[A] =>
  //        o.length == length &&
  //        o.rowVector == rowVector &&
  //        compare(o)
  //
  //      case _ => false
  //    }
  //  }
}
